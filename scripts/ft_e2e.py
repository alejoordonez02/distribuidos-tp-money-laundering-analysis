"""End-to-end fault-tolerance test (`make test_ft`).

Brings the cluster up ONCE and drives one fresh client through it per crash point:
arm the target node with the fault, send a client, let the restart policy recover,
wait for the system to fully drain, and check the result matches the oracle. Each
client mints a new client_id (and a producer_id derived from it), so consecutive
clients never collide in any downstream dedup table — the system is reused safely
instead of being torn down and rebuilt 30 times (repeated cold starts of the whole
topology on a busy host were the source of false stalls).

Only the target node is recreated between combos (to swap its FAULT_CRASH_POINT env,
which is fixed at container creation); the other ~70 containers stay warm, so the
host load stays flat and the stall detector sees real deadlocks, not starvation.

Topology and replica counts are read from the generated docker-compose.yaml. Dataset
comes from scripts/cfg.py.

Env knobs:
  FT_ONLY_NODES=a,b   restrict to these controller types (or exact names)
  FT_SKIP_NODES=a,b   skip these controller types (or exact names)
  FT_ONLY_POINTS=p,q  restrict to these crash points
  FT_ALL_REPLICAS=1   crash every replica, not just idx 0
  FT_TIMEOUT=300      per-client seconds before declaring a stall (hard cap)
  FT_STALL_GRACE=90   queue-frozen seconds (target node UP) that mean a deadlock
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

# Per-client hard cap. The real stall detector is progress-based (a run still
# draining queues is slow, not deadlocked), and is paused while the target node is
# down (a frozen queue is then expected, not a deadlock).
TIMEOUT = int(os.getenv("FT_TIMEOUT", "300"))
STALL_GRACE = int(os.getenv("FT_STALL_GRACE", "90"))
PROGRESS_INTERVAL = int(os.getenv("FT_PROGRESS_INTERVAL", "5"))
# After a client finishes, wait for every queue to drain before the next client, so
# no leftover EOF/ring message bleeds across clients.
DRAIN_TIMEOUT = int(os.getenv("FT_DRAIN_TIMEOUT", "120"))
# retry a stalled combo once (cheap now: re-arm + one client, no full rebuild).
STALL_RETRY = os.getenv("FT_STALL_RETRY", "1") == "1"
KILL_DELAY = int(os.getenv("FT_KILL_DELAY", "8"))
# `docker kill` is treated by the daemon as a manual stop, so `restart: on-failure`
# does NOT bring the container back (unlike a real process crash / OOM kill). To
# simulate the orchestrator restarting a hard-crashed node we `docker start` it after
# the kill; the recovery-path crash point then fires during restore.
KILL_RESTART_GRACE = int(os.getenv("FT_KILL_RESTART_GRACE", "2"))
ALL_REPLICAS = os.getenv("FT_ALL_REPLICAS") == "1"
SKIP_GEN = os.getenv("FT_SKIP_GEN") == "1"
ORACLE_MEM_MAX = os.getenv("FT_ORACLE_MEM", "5G")
# One-time warmup for the whole topology to connect + bind before the first client.
STARTUP_GRACE = int(os.getenv("FT_STARTUP_GRACE", "20"))
# Let a freshly recreated target node reconnect + re-declare its queues before the
# client starts publishing (messages queue durably meanwhile, so nothing is lost).
NODE_READY_GRACE = int(os.getenv("FT_NODE_READY_GRACE", "3"))
CLIENT = os.getenv("FT_CLIENT", "client_0")


def run(cmd, check=False, capture=False, timeout=None):
    try:
        return subprocess.run(
            cmd, shell=True, cwd=ROOT, check=check,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.STDOUT if capture else None,
            text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")


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


def non_client_services():
    services = out(f"docker compose -f {COMPOSE} config --services").splitlines()
    return [s.strip() for s in services if s.strip() and not s.strip().startswith("client")]


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


def clean_all_state():
    """Wipe every node's persisted state (root-owned, written in-container) so the
    cluster starts from zero — otherwise stale checkpoints from a previous run are
    restored on bring-up and pollute the first client."""
    run(f'docker run --rm -v "{os.path.join(ROOT, "state")}:/state" alpine '
        "find /state -mindepth 1 -delete", capture=True)


def compose_up_cluster():
    """Bring up rabbitmq + gateway + every controller once, without any client and
    without any fault override (the cluster starts clean)."""
    svcs = " ".join(non_client_services())
    run(f"docker compose -f {COMPOSE} up -d --remove-orphans {svcs}", capture=True)


def recreate_node(node):
    """Recreate just the target node with the fault override applied (its
    FAULT_CRASH_POINT is fixed at container creation, so a swap needs a recreate)."""
    run(f"docker compose -f {COMPOSE} -f {FAULT} up -d --force-recreate --no-deps {node}",
        capture=True)


def recreate_client():
    """Run one fresh client through the live cluster (new client_id + producer_id)."""
    run(f"docker compose -f {COMPOSE} up -d --force-recreate --no-deps {CLIENT}",
        capture=True)


def clear_fault_sentinel(node):
    """Re-arm the one-shot fault by removing ONLY the sentinel, KEEPING the checkpoint.

    The checkpoint holds the node's output `seq` counter. Wiping it would reset the
    seq to 0 while downstream dedup tables (which are NOT wiped — only the target node
    is recreated each combo) still hold the previous client's high-water mark, so the
    recreated node's re-numbered output would be silently deduped downstream and starve
    the pipeline. Keeping the checkpoint keeps the seq monotonic across clients, exactly
    as in normal operation where a node is never wiped."""
    path = os.path.join(ROOT, "state", node)
    run(f'docker run --rm -v "{path}:/state" alpine rm -f /state/.fault_fired',
        capture=True)


def clear_responses():
    resp = os.path.join(ROOT, "responses")
    if os.path.isdir(resp):
        for f in os.listdir(resp):
            if f.endswith(".csv"):
                os.remove(os.path.join(resp, f))


def node_running(node):
    return out(f'docker inspect -f "{{{{.State.Running}}}}" {node}') == "true"


def clients_running():
    return bool(out('docker ps --filter "name=client_" --filter "status=running" -q'))


def queue_total():
    """Best-effort in-flight message total across all queues — a cheap progress
    proxy. Returns None when the broker is too busy to answer (don't penalize that;
    a missing reading is treated as 'no change', never as a stall by itself)."""
    r = run(
        "docker exec rabbitmq rabbitmqctl list_queues messages --quiet",
        capture=True, timeout=15,
    )
    nums = [int(tok) for tok in r.stdout.split() if tok.isdigit()]
    return sum(nums) if nums else None


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


def wait_for_client(node, start):
    """Progress-based stall detection on the live cluster. A run still draining
    queues is slow (load), not deadlocked. While the target node is down (crashed /
    restarting) a frozen queue is EXPECTED, so the freeze timer is paused — only a
    queue frozen with the target node UP and work left is a real logical deadlock."""
    last_total, frozen_since = None, time.time()
    while clients_running():
        time.sleep(PROGRESS_INTERVAL)
        if time.time() - start > TIMEOUT:
            return False
        if not node_running(node):
            frozen_since = time.time()  # node restarting: freezing is not a deadlock
            continue
        total = queue_total()
        if total is None:
            continue  # unreadable broker: neither progress nor stall evidence
        if total != last_total:
            last_total, frozen_since = total, time.time()  # still moving
        elif time.time() - frozen_since > STALL_GRACE:
            return False  # frozen with the node up and work left -> logical deadlock
    return True


def wait_drain():
    """After a client finishes, wait for every queue to empty so no leftover message
    bleeds into the next client. Returns False if it never fully drains in time."""
    deadline = time.time() + DRAIN_TIMEOUT
    while time.time() < deadline:
        if queue_total() == 0:
            return True
        time.sleep(2)
    return False


def run_combo(node, point):
    is_kill = point in KILL_POINTS
    clear_responses()
    clear_fault_sentinel(node)
    write_fault(node, point)
    recreate_node(node)
    time.sleep(NODE_READY_GRACE)

    start = time.time()
    recreate_client()
    if is_kill:
        time.sleep(KILL_DELAY)
        run(f"docker kill --signal=KILL {node}", capture=True)
        time.sleep(KILL_RESTART_GRACE)
        run(f"docker start {node}", capture=True)

    completed = wait_for_client(node, start)
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

    wait_drain()  # settle the cluster before the next client
    return {
        "node": node, "point": point, "fired": fired,
        "result": result, "detail": detail, "secs": secs,
    }


def prepare():
    # self-crash points rely on docker's restart policy to recover the node, so the
    # e2e opts back into it (it is off by default, where the supervisor revives).
    run(
        f"GEN_RESTART_ON_FAILURE=1 uv run -m scripts.gen_compose.gen_compose {COMPOSE}",
        capture=True,
    )
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
    print(f"[ft] one-cluster mode: {len(combos)} combos "
          f"({len(targets)} nodes x {len(points)} points), per-client cap {TIMEOUT}s\n",
          flush=True)

    print("[ft] bringing up the cluster once ...", flush=True)
    clean_all_state()
    compose_up_cluster()
    time.sleep(STARTUP_GRACE)

    results = []
    try:
        for i, (node, point) in enumerate(combos, 1):
            print(f"[{i}/{len(combos)}] {node} @ {point} ...", flush=True)
            res = run_combo(node, point)
            if res["result"] == "STALL" and STALL_RETRY:
                print(f"      -> STALL ({res['secs']}s); retrying once ...", flush=True)
                res = run_combo(node, point)
            results.append(res)
            flag = "" if res["result"] == "PASS" else "  <<<"
            note = "" if res["fired"] else " (crash did not fire — point not reached)"
            print(f"      -> {res['result']} ({res['secs']}s){note}{flag}", flush=True)
    finally:
        print("\n[ft] tearing down the cluster ...", flush=True)
        run(f"docker compose -f {COMPOSE} -f {FAULT} down --remove-orphans", capture=True)

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
