"""End-to-end scalability test (`make scalability_test`).

Sibling of the fault-tolerance e2e (ft_e2e.py): it shares the same cluster-driving helpers
(ft_common.py) but instead of crashing nodes it scales the rings. For each topology it
rewrites the ring sizes, brings the cluster up, runs one client per dataset tier through it
and checks the result matches the cached oracle. It proves the affinity rings stay correct
as any stage grows from a single node to a ring of 2 or 3.

The deadlock this guards against was timing-dependent, so each (topology, tier) can be run
several times. On a stall it dumps the stuck queues, the dead nodes' exit codes and the
surrounding node logs so a regression is easy to localise.

Topology and dataset are read from scripts/gen_compose/src/topology.py and scripts/cfg.py,
which this test rewrites per run and restores on exit. NCLIENTS stays 1 so the cached
per-tier oracle in test/expected_cache/<tier> applies without regenerating it.

Env knobs:
  SCALE_TOPOS=current,min2,...   topologies to run (default current,min2,bottleneck3)
  SCALE_TIERS=perfect,small,...  dataset tiers to run (default perfect,small,medium,large)
  SCALE_REPEAT=2                 runs per (topology, tier)
  SCALE_STALL_GRACE=120          seconds of frozen queues (whole cluster up) meaning a deadlock
"""

import os
import re
import sys
import time
from datetime import datetime

from ft_common import (
    COMPOSE,
    ROOT,
    clean_all_state,
    clear_responses,
    clients_running,
    compose_up_cluster,
    out,
    queue_total,
    recreate_client,
    run,
    verify,
)

CFG = os.path.join(ROOT, "scripts/cfg.py")
TOPOLOGY = os.path.join(ROOT, "scripts/gen_compose/src/topology.py")
DATASETS = os.path.join(ROOT, "datasets")
OUT_DIR = os.path.join(ROOT, "tmp/scalability")

STALL_GRACE = int(os.getenv("SCALE_STALL_GRACE", "120"))
STARTUP_GRACE = int(os.getenv("SCALE_STARTUP_GRACE", "25"))
PROGRESS_INTERVAL = int(os.getenv("SCALE_PROGRESS", "5"))

TIERS = {
    "perfect": ("datasets/perfect_sample.csv", "datasets/perfect_sample_accounts.csv", None),
    "small": ("datasets/LI-Small_Trans.csv", "datasets/LI-Small_accounts.csv", "small"),
    "medium": ("datasets/HI-Medium_Trans.csv", "datasets/HI-Medium_accounts.csv", "medium"),
    "large": ("datasets/HI-Large_Trans.csv", "datasets/HI-Large_accounts.csv", "large"),
}

TIER_CAP = {
    "perfect": int(os.getenv("SCALE_CAP_PERFECT", "900")),
    "small": int(os.getenv("SCALE_CAP_SMALL", "1800")),
    "medium": int(os.getenv("SCALE_CAP_MEDIUM", "3600")),
    "large": int(os.getenv("SCALE_CAP_LARGE", "7200")),
}

_KEYS = [
    "DEFAULT_FILTERS",
    "UC2_MAX_AMOUNT_GROUP_BYS", "UC2_MAX_AMOUNT_AGGREGATES",
    "UC2_BANK_NAMES_GROUP_BYS", "UC2_BANK_NAMES_AGGREGATES", "UC2_MERGES",
    "UC3_GROUP_BYS", "UC3_AGGREGATES", "UC3_MERGES", "UC3_FILTERS",
    "UC4_COMPUTE_GRAPHS", "UC4_AGGREGATE_GRAPHS", "UC4_DEGREE_AGGREGATES",
    "UC4_PRUNES", "UC4_COUNT_PATHS", "UC4_PATHS_AGGREGATES",
    "UC5_CONVERTERS", "UC5_AMOUNT_FILTERS", "UC5_COUNT_GROUP_BYS",
]

_CURRENT = {
    "DEFAULT_FILTERS": 2,
    "UC2_MAX_AMOUNT_GROUP_BYS": 1, "UC2_MAX_AMOUNT_AGGREGATES": 1,
    "UC2_BANK_NAMES_GROUP_BYS": 1, "UC2_BANK_NAMES_AGGREGATES": 1, "UC2_MERGES": 2,
    "UC3_GROUP_BYS": 1, "UC3_AGGREGATES": 1, "UC3_MERGES": 2, "UC3_FILTERS": 1,
    "UC4_COMPUTE_GRAPHS": 2, "UC4_AGGREGATE_GRAPHS": 1, "UC4_DEGREE_AGGREGATES": 1,
    "UC4_PRUNES": 2, "UC4_COUNT_PATHS": 2, "UC4_PATHS_AGGREGATES": 2,
    "UC5_CONVERTERS": 1, "UC5_AMOUNT_FILTERS": 1, "UC5_COUNT_GROUP_BYS": 1,
}

JOIN_PARTITION_BLOCK = (
    "\nJOIN_PARTITION = [\n"
    "    [1],\n"
    "    [3],\n"
    "    [2, 4, 5],\n"
    "]\n"
)


def _variant(**overrides):
    v = dict(_CURRENT)
    v.update(overrides)
    return v


TOPOLOGIES = {
    "current": dict(_CURRENT),
    "min2": {k: max(2, _CURRENT[k]) for k in _KEYS},
    "min3": {k: max(3, _CURRENT[k]) for k in _KEYS},
    "bottleneck3": _variant(
        UC4_COUNT_PATHS=3, UC4_PATHS_AGGREGATES=3, UC4_AGGREGATE_GRAPHS=3,
        UC3_AGGREGATES=3, UC2_MAX_AMOUNT_AGGREGATES=2, UC2_BANK_NAMES_AGGREGATES=2,
    ),
    "filters3": _variant(DEFAULT_FILTERS=3),
    "uc2agg2": _variant(UC2_MAX_AMOUNT_AGGREGATES=2, UC2_BANK_NAMES_AGGREGATES=2),
    "uc3agg3": _variant(UC3_AGGREGATES=3),
    "uc3gb3": _variant(UC3_GROUP_BYS=3),
    "uc4agg3": _variant(UC4_AGGREGATE_GRAPHS=3),
    "uc4cp3": _variant(UC4_COUNT_PATHS=3, UC4_PATHS_AGGREGATES=3),
}


def render_topology(values):
    lines = [f"DEFAULT_FILTERS = {values['DEFAULT_FILTERS']}", "", "# UC2"]
    lines += [f"{k} = {values[k]}" for k in _KEYS[1:6]]
    lines += ["", "# UC3"] + [f"{k} = {values[k]}" for k in _KEYS[6:10]]
    lines += ["", "# UC4"] + [f"{k} = {values[k]}" for k in _KEYS[10:16]]
    lines += ["", "# UC5"] + [f"{k} = {values[k]}" for k in _KEYS[16:19]]
    lines += [""]
    return "\n".join(lines) + JOIN_PARTITION_BLOCK


def render_cfg(trans, accts):
    return (
        f'TRANSACTIONS_PATH = "{trans}"\n'
        f'ACCOUNTS_PATH = "{accts}"\n'
        "ACCOUNTS_SAMPLE_SIZE = None\n\n"
        "NCLIENTS = 1\n"
        "TRANSACTIONS_SAMPLE_FRAC: float = 1 / NCLIENTS\n"
        'CLIENT_DATASETS_PATH = "datasets/"\n'
        'CLIENT_EXPECTED_RESPONSES_PATH = "test/expected_responses/"\n\n'
        'CLIENT_RESPONSES_PATH = "responses/"\n'
    )


_ORIGINALS = {}


def _snapshot(path):
    with open(path) as f:
        _ORIGINALS[path] = f.read()


def _restore_all():
    for path, content in _ORIGINALS.items():
        with open(path, "w") as f:
            f.write(content)


def write_topology(name):
    with open(TOPOLOGY, "w") as f:
        f.write(render_topology(TOPOLOGIES[name]))


def setup_tier(tier):
    trans, accts, cache = TIERS[tier]
    with open(CFG, "w") as f:
        f.write(render_cfg(trans, accts))
    link = os.path.join(DATASETS, "transactions_0.csv")
    if os.path.lexists(link):
        os.remove(link)
    os.symlink(os.path.basename(trans), link)
    if cache is None:
        r = run("PYTHONPATH=src uv run scripts/gen_input_output.py", capture=True)
        if r.returncode != 0:
            raise RuntimeError(f"gen_input_output failed for {tier}: {r.stdout[-400:]}")
    else:
        src = os.path.join(ROOT, "test/expected_cache", cache)
        dst = os.path.join(ROOT, "test/expected_responses")
        os.makedirs(dst, exist_ok=True)
        run(f"cp {src}/*.csv {dst}/", capture=True)


def gen_compose():
    run(f"uv run -m scripts.gen_compose.gen_compose {COMPOSE}", capture=True)


def teardown():
    run(f"docker compose -f {COMPOSE} down --remove-orphans -t 5", capture=True)


_built = False


def bring_up(topology):
    global _built
    teardown()
    write_topology(topology)
    gen_compose()
    if not _built:
        log("building images (one-time) ...")
        run(f"docker compose -f {COMPOSE} build", capture=True)
        _built = True
    clean_all_state()
    compose_up_cluster()
    time.sleep(STARTUP_GRACE)


def expected_controllers():
    services = out(f"docker compose -f {COMPOSE} config --services").splitlines()
    infra = {"rabbitmq", "gateway", "supervisor", "chaos"}
    return [s.strip() for s in services
            if s.strip() and s.strip() not in infra and not s.strip().startswith("client")]


def running_names():
    return set(out('docker ps --format "{{.Names}}"').split())


def dump_queues():
    return out("docker exec rabbitmq rabbitmqctl list_queues name messages consumers --quiet",
               timeout=25)


def inspect_dead(dead_nodes):
    lines = []
    for n in dead_nodes:
        info = out(f'docker inspect -f "exit={{{{.State.ExitCode}}}} '
                   f'OOMKilled={{{{.State.OOMKilled}}}} status={{{{.State.Status}}}}" {n}',
                   timeout=10)
        lines.append(f"  {n}: {info}")
    return "\n".join(lines)


def capture_node_logs(stuck_lines, extra_nodes=(), tail=150):
    ucs = set()
    for ln in stuck_lines:
        parts = ln.split()
        if parts:
            m = re.match(r"(uc\d|default|join)", parts[0])
            if m:
                ucs.add(m.group(1))
    names = out('docker ps -a --format "{{.Names}}"').split()
    targets = sorted(set(
        [n for n in names if any(n.startswith(u) for u in ucs)]
        + [n for n in extra_nodes if n in names]
    ))
    if not targets:
        return ""
    return "\n\n".join(
        f"===== {n} =====\n{out(f'docker logs --tail {tail} {n} 2>&1', timeout=15)}"
        for n in targets
    )


def wait_for_completion(tier, expected):
    """No node is crashed here, so the cluster should never lose a controller. Reports
    completion, a controller that died on its own, a queue frozen with the whole cluster up
    (a logical deadlock), or the hard cap."""
    cap = TIER_CAP[tier]
    start = time.time()
    last, frozen_since = None, time.time()
    while clients_running():
        time.sleep(PROGRESS_INTERVAL)
        if time.time() - start > cap:
            return False, "cap_timeout"
        down = [c for c in expected if c not in running_names()]
        if down:
            return False, f"node_died:{','.join(sorted(down)[:6])}"
        total = queue_total()
        if total == 0:
            frozen_since = time.time()
            continue
        if total is None:
            continue
        if total != last:
            last, frozen_since = total, time.time()
        elif time.time() - frozen_since > STALL_GRACE:
            return False, f"deadlock_frozen_at_{total}_msgs"
    return True, "completed"


def write_diag(topology, tier, reason, secs, validated, detail):
    diag = dump_queues()
    stuck = [ln for ln in diag.splitlines()
             if len(ln.split()) >= 2 and ln.split()[1].isdigit() and int(ln.split()[1]) > 0]
    dead = reason.split(":", 1)[1].split(",") if reason.startswith("node_died") else []
    dead = [d for d in dead if d]
    path = os.path.join(OUT_DIR, f"diag_{topology}_{tier}.txt")
    with open(path, "w") as f:
        f.write(f"topology={topology} tier={tier} reason={reason} secs={secs}\n")
        f.write(f"validated={validated} detail={detail}\n\n")
        if dead:
            f.write("=== dead nodes ===\n" + inspect_dead(dead) + "\n\n")
        f.write("=== queues (name messages consumers) ===\n" + diag + "\n\n")
        f.write("=== node logs ===\n" + capture_node_logs(stuck, extra_nodes=dead) + "\n")
    return stuck, os.path.basename(path)


def probe(topology, tier):
    bring_up(topology)
    clear_responses()
    start = time.time()
    recreate_client()
    ok, reason = wait_for_completion(tier, expected_controllers())
    secs = int(time.time() - start)
    validated, detail = (verify() if ok else (False, ""))
    result = "PASS" if (ok and validated) else ("DEADLOCK" if not ok else "WRONG_RESULT")
    if result != "PASS":
        stuck, diag_name = write_diag(topology, tier, reason, secs, validated, detail)
        log(f"    {result} ({secs}s) {reason}; stuck queues: {len(stuck)} (see {diag_name})")
        for ln in stuck[:10]:
            log(f"        {ln}")
    else:
        log(f"    PASS ({secs}s) 5/5")
    return {"topology": topology, "tier": tier, "result": result, "reason": reason, "secs": secs}


def verify_clean():
    leftovers = []
    containers = [c for c in out(f"docker compose -f {COMPOSE} ps -aq").split() if c]
    if containers:
        leftovers.append(f"{len(containers)} containers up")
    state = [x for x in out(f'find "{os.path.join(ROOT, "state")}" -mindepth 1',
                            timeout=15).splitlines() if x]
    if state:
        leftovers.append(f"{len(state)} disk state entries")
    resp = os.path.join(ROOT, "responses")
    csv = [f for f in os.listdir(resp) if f.endswith(".csv")] if os.path.isdir(resp) else []
    if csv:
        leftovers.append(f"{len(csv)} response files")
    log("CLEANUP WARNING: " + "; ".join(leftovers) if leftovers
        else "cleanup verified: no containers, no disk state, no responses, queues gone")


def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(os.path.join(OUT_DIR, "scalability.log"), "a") as f:
        f.write(line + "\n")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    topos = [t.strip() for t in os.getenv("SCALE_TOPOS", "current,min2,bottleneck3").split(",")
             if t.strip()]
    tiers = [t.strip() for t in os.getenv("SCALE_TIERS", "perfect,small,medium,large").split(",")
             if t.strip()]
    repeat = int(os.getenv("SCALE_REPEAT", "2"))
    log(f"scalability: topos={topos} tiers={tiers} repeat={repeat}")

    _snapshot(CFG)
    _snapshot(TOPOLOGY)
    results = []
    try:
        for tier in tiers:
            for topology in topos:
                if topology not in TOPOLOGIES:
                    log(f"unknown topology '{topology}', skipping")
                    continue
                setup_tier(tier)
                for _ in range(repeat):
                    log(f"--- {topology} @ {tier} ---")
                    try:
                        results.append(probe(topology, tier))
                    except Exception as e:
                        log(f"    EXCEPTION {topology}@{tier}: {type(e).__name__}: {e}")
                        results.append({"topology": topology, "tier": tier,
                                        "result": "EXCEPTION", "reason": str(e), "secs": 0})
    finally:
        teardown()
        clean_all_state()
        clear_responses()
        _restore_all()
        verify_clean()

    agg = {}
    for r in results:
        agg.setdefault((r["topology"], r["tier"]), []).append(r)
    failures = 0
    log("==================== SCALABILITY SUMMARY ====================")
    for (topology, tier), rs in sorted(agg.items()):
        npass = sum(1 for r in rs if r["result"] == "PASS")
        mark = "OK " if npass == len(rs) else "XX "
        if npass != len(rs):
            failures += 1
        detail = "; ".join(sorted({r["reason"] for r in rs if r["result"] != "PASS"}))
        log(f"  {mark}{topology:<14} {tier:<8} {npass}/{len(rs)} pass  {detail}")
    log("============================================================")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
