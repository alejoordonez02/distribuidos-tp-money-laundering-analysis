"""End-to-end scalability test (`make scalability_test`).

Sibling of the fault-tolerance e2e (ft_e2e.py): it shares the cluster-driving helpers
(ft_common.py) but instead of crashing nodes it scales the rings. For each topology it
rewrites the ring sizes, brings the cluster up, runs one client per dataset tier through it
and checks the result matches the cached oracle. It proves the affinity rings stay correct
as any stage grows from a single node to a ring of 2 or 3.

The deadlock this guards against was timing-dependent, so each (topology, tier) can be run
several times. On a stall it dumps the stuck queues, the dead nodes' exit codes and the
surrounding node logs so a regression is easy to localise.

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
    CURRENT,
    ROOT,
    TIER_CAP,
    TOPOLOGY_KEYS,
    bring_up_cluster,
    clean_all_state,
    clear_responses,
    clients_running,
    expected_controllers,
    out,
    queue_total,
    recreate_client,
    restore_files,
    running_names,
    setup_tier,
    snapshot_files,
    teardown,
    variant,
    verify,
    write_topology,
)

OUT_DIR = os.path.join(ROOT, "tmp/scalability")
STALL_GRACE = int(os.getenv("SCALE_STALL_GRACE", "120"))
PROGRESS_INTERVAL = int(os.getenv("SCALE_PROGRESS", "5"))

TOPOLOGIES = {
    "current": dict(CURRENT),
    "min2": {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS},
    "min3": {k: max(3, CURRENT[k]) for k in TOPOLOGY_KEYS},
    "bottleneck3": variant(
        UC4_COUNT_PATHS=3, UC4_PATHS_AGGREGATES=3, UC4_AGGREGATE_GRAPHS=3,
        UC3_AGGREGATES=3, UC2_MAX_AMOUNT_AGGREGATES=2, UC2_BANK_NAMES_AGGREGATES=2,
    ),
    "filters3": variant(DEFAULT_FILTERS=3),
    "uc2agg2": variant(UC2_MAX_AMOUNT_AGGREGATES=2, UC2_BANK_NAMES_AGGREGATES=2),
    "uc3agg3": variant(UC3_AGGREGATES=3),
    "uc3gb3": variant(UC3_GROUP_BYS=3),
    "uc4agg3": variant(UC4_AGGREGATE_GRAPHS=3),
    "uc4cp3": variant(UC4_COUNT_PATHS=3, UC4_PATHS_AGGREGATES=3),
}

_built = False


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
    global _built
    write_topology(TOPOLOGIES[topology])
    bring_up_cluster(do_build=not _built)
    _built = True
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

    snapshot_files()
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
        restore_files()
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
