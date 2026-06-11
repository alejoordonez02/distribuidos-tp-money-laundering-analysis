"""End-to-end fault-tolerance test (`make test_ft`).

Crashes each controller at each crash point, lets the restart policy recover it,
and checks the system still produces correct results. Topology and replica counts
are read from the generated docker-compose.yaml. Dataset comes from scripts/cfg.py.

Env knobs:
  FT_ONLY_NODES=a,b   restrict to these controller types (or exact names)
  FT_SKIP_NODES=a,b   skip these controller types (or exact names)
  FT_ONLY_POINTS=p,q  restrict to these crash points
  FT_ALL_REPLICAS=1   crash every replica, not just idx 0
  FT_TIMEOUT=120      per-run seconds before declaring a stall
  FT_KILL_DELAY=8     seconds before the external kill (recovery-path points)
  FT_SKIP_GEN=1       skip gen_input_output (reuse expected responses)
"""

import os
import shutil
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = "docker-compose.yaml"
FAULT = "tmp/ft_run/ft_e2e_fault.yaml"

# Points that crash during normal processing (fire within a single fresh run).
SELF_POINTS = [
    "after_apply_before_checkpoint",
    "during_checkpoint_write",
    "after_checkpoint_before_ack",
    "before_eof_flush",
    "after_eof_flush_before_handle",
    "after_eof_handle_before_ack",
    "after_ring_eof_forward_before_ack",
    "after_downstream_eof_before_ack",
]
# recovery-path points: need an external kill to bootstrap a restart before they fire
KILL_POINTS = [
    "after_restore_on_startup",
    "after_dup_before_ack",
]
ALL_POINTS = SELF_POINTS + KILL_POINTS

TIMEOUT = int(os.getenv("FT_TIMEOUT", "120"))
KILL_DELAY = int(os.getenv("FT_KILL_DELAY", "8"))
ALL_REPLICAS = os.getenv("FT_ALL_REPLICAS") == "1"
SKIP_GEN = os.getenv("FT_SKIP_GEN") == "1"
ORACLE_MEM_MAX = os.getenv("FT_ORACLE_MEM", "5G")
STARTUP_GRACE = 5


def run(cmd, check=False, capture=False):
    return subprocess.run(
        cmd, shell=True, cwd=ROOT, check=check,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        text=True,
    )


def out(cmd):
    return run(cmd, capture=True).stdout.strip()


def discover_controllers():
    """Read the compose and return [(type, [replica names], count)] for every
    controller (everything except rabbitmq, the gateway and the clients)."""
    services = out(f"docker compose -f {COMPOSE} config --services").splitlines()
    services = [s.strip() for s in services if s.strip()]
    groups: dict[str, list[str]] = {}
    for name in sorted(services):
        if name == "rabbitmq" or name == "gateway" or name.startswith("client"):
            continue
        type_ = name
        if "_" in name and name.rsplit("_", 1)[1].isdigit():
            type_ = name.rsplit("_", 1)[0]  # strip the _<n> replica suffix
        groups.setdefault(type_, []).append(name)
    return [(t, sorted(names, key=_idx), len(names)) for t, names in sorted(groups.items())]


def _idx(name):
    tail = name.rsplit("_", 1)[1] if "_" in name else ""
    return int(tail) if tail.isdigit() else -1


def write_fault(node, point):
    os.makedirs(os.path.join(ROOT, "tmp/ft_run"), exist_ok=True)
    with open(os.path.join(ROOT, FAULT), "w") as f:
        f.write(
            "services:\n"
            f"  {node}:\n"
            "    environment:\n"
            "      - FAULT_INJECTION=1\n"
            f"      - FAULT_CRASH_POINT={point}\n"
        )


def clean_state():
    # state/ is root-owned (written in-container); wipe via a throwaway container
    run(f'docker run --rm -v "{ROOT}/state:/state" alpine sh -c "rm -rf /state/*"', capture=True)
    resp = os.path.join(ROOT, "responses")
    if os.path.isdir(resp):
        for f in os.listdir(resp):
            if f.endswith(".csv"):
                os.remove(os.path.join(resp, f))


def compose(action):
    run(f"docker compose -f {COMPOSE} -f {FAULT} {action}", capture=True)


def clients_running():
    return bool(out('docker ps --filter "name=client_" --filter "status=running" -q'))


def wait_for_completion(start):
    while clients_running():
        time.sleep(2)
        if time.time() - start > TIMEOUT:
            return False
    return True


def client_exit_codes():
    names = out('docker ps -a --filter "name=client_" --format "{{.Names}}"').split()
    codes = []
    for n in names:
        codes.append(out(f'docker inspect -f "{{{{.State.ExitCode}}}}" {n}'))
    return codes


def verify():
    r = run(
        "uv run pytest test/test_uc1.py test/test_uc2.py test/test_uc3.py "
        "test/test_uc4.py test/test_uc5.py -q",
        capture=True,
    )
    tail = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
    return r.returncode == 0, tail


def sentinel_fired(node):
    return os.path.exists(os.path.join(ROOT, "state", node, ".fault_fired"))


def run_combo(node, point):
    is_kill = point in KILL_POINTS
    write_fault(node, point)
    clean_state()
    start = time.time()
    compose("up --remove-orphans -d")
    time.sleep(STARTUP_GRACE)
    if is_kill:
        time.sleep(KILL_DELAY)
        run(f"docker kill --signal=KILL {node}", capture=True)
    completed = wait_for_completion(start)
    secs = int(time.time() - start)
    fired = sentinel_fired(node)

    if not completed:
        result, detail = "STALL", "client did not finish"
    else:
        codes = client_exit_codes()
        if any(c != "0" for c in codes):
            result, detail = "NO_COMPLETE", f"client exit {codes}"
        else:
            ok, tail = verify()
            result, detail = ("PASS" if ok else "FAIL"), tail
    compose("down --remove-orphans")
    return {
        "node": node, "point": point, "fired": fired,
        "result": result, "detail": detail, "secs": secs,
    }


def prepare():
    run(f"uv run -m scripts.gen_compose.gen_compose {COMPOSE}", capture=True)
    if not SKIP_GEN:
        print("[ft] generating expected responses (oracle) once ...", flush=True)
        guard = ""
        if shutil.which("systemd-run"):
            # cap memory so a heavy dataset OOM-kills the oracle, never the host
            guard = (
                f"systemd-run --user --scope -p MemoryMax={ORACLE_MEM_MAX} "
                "-p MemorySwapMax=0 --quiet -- "
            )
        r = run(f'{guard}bash -c "PYTHONPATH=src uv run scripts/gen_input_output.py"', capture=True)
        if r.returncode != 0:
            print(r.stdout)
            sys.exit("[ft] gen_input_output failed (out of memory? point cfg.py at a "
                     "smaller dataset, or set FT_SKIP_GEN=1 to reuse expected responses)")


def main():
    prepare()
    controllers = discover_controllers()
    print("[ft] topology discovered from compose:")
    for type_, names, count in controllers:
        print(f"      {type_}: {count} replica(s)")

    only_nodes = set(filter(None, os.getenv("FT_ONLY_NODES", "").split(",")))
    skip_nodes = set(filter(None, os.getenv("FT_SKIP_NODES", "").split(",")))
    only_points = set(filter(None, os.getenv("FT_ONLY_POINTS", "").split(",")))
    points = [p for p in ALL_POINTS if not only_points or p in only_points]

    targets = []
    for type_, names, count in controllers:
        if type_ in skip_nodes or set(names) & skip_nodes:
            continue
        if only_nodes and type_ not in only_nodes and not (set(names) & only_nodes):
            continue
        chosen = names if ALL_REPLICAS else names[:1]
        if only_nodes:
            chosen = [n for n in names if n in only_nodes] or chosen
        for n in chosen:
            targets.append(n)

    combos = [(n, p) for n in targets for p in points]
    print(f"[ft] running {len(combos)} combos "
          f"({len(targets)} nodes x {len(points)} points), timeout {TIMEOUT}s\n", flush=True)

    results = []
    for i, (node, point) in enumerate(combos, 1):
        print(f"[{i}/{len(combos)}] {node} @ {point} ...", flush=True)
        res = run_combo(node, point)
        results.append(res)
        flag = "" if res["result"] == "PASS" else "  <<<"
        note = "" if res["fired"] else " (crash did not fire — point not reached)"
        print(f"      -> {res['result']} ({res['secs']}s){note}{flag}", flush=True)

    failures = [r for r in results if r["result"] != "PASS" and r["fired"]]
    notfired = [r for r in results if not r["fired"]]
    print("\n==================== FT E2E SUMMARY ====================")
    print(f"total: {len(results)}  pass: {sum(r['result']=='PASS' for r in results)}  "
          f"real-failures: {len(failures)}  not-fired: {len(notfired)}")
    if failures:
        print("\nREAL FAILURES (crash fired but recovery was wrong):")
        for r in failures:
            print(f"  {r['node']} @ {r['point']} -> {r['result']}  [{r['detail']}]")
    if notfired:
        print(f"\nNot exercised ({len(notfired)} combos — that crash point is not on "
              f"this node's code path, e.g. the EOF reached another replica):")
        for r in notfired:
            print(f"  {r['node']} @ {r['point']}")
    print("=======================================================")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
