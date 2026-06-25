"""Classify the i2 (interval=2s, ~120 kills/min) cliff: DEADLOCK vs LIVELOCK.

The full FT-vs-perf benchmark shows a reproducible cliff at interval=2s on both
small and medium: the pipeline wedges and never finishes within the 1800s cap, on
BOTH seed 1234 and 1235, while every gentler rate (i5 and up) validates 5/5. The
wedge signature is telling: ~7 revives per kill and the queue drained to near zero
with controllers still down — that smells like churn, not mute saturation.

This isolates the cliff on small (fastest to wedge) and answers the decisive
question with a controlled intervention:

  1. fresh small/min2/ckpt1000 cluster, arm chaos i2 k4 seed 1234
  2. run a client; if it exceeds WEDGE_TIMEOUT without finishing -> wedged
  3. on wedge: DISARM chaos (stop killing), snapshot down-nodes/queue, then watch
     for RECOVER_WINDOW seconds whether the queue drains and the client finishes:
       - finishes -> LIVELOCK: recovery simply can't keep up at 120/min; the cliff
                     is a real throughput limit, not a bug. The system self-heals
                     the instant the kill pressure stops.
       - stuck    -> DEADLOCK: a genuine bug. Dump down-node + supervisor logs so
                     we can see what state the ring/EOF machinery is wedged in.

    PYTHONPATH=src uv run scripts/diagnose_i2_wedge.py

Do NOT run while the benchmark is running — they share the host. Cleanup:
    docker compose -f docker-compose.yaml down --remove-orphans
    git checkout scripts/cfg.py scripts/gen_compose/src/topology.py
"""

import os
import time

from ft_common import (
    COMPOSE,
    CURRENT,
    TOPOLOGY_KEYS,
    bring_up_cluster,
    clear_responses,
    clients_running,
    expected_controllers,
    out,
    queue_total,
    recreate_client,
    run,
    running_names,
    setup_tier,
    write_topology,
)

TIER = os.getenv("DIAG_TIER", "small")
SEED = int(os.getenv("DIAG_SEED", "1234"))
INTERVAL = int(os.getenv("DIAG_INTERVAL", "2"))
KILLS = int(os.getenv("DIAG_KILLS", "4"))
CHECKPOINT = int(os.getenv("DIAG_CHECKPOINT", "1000"))
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
WEDGE_TIMEOUT = int(os.getenv("DIAG_WEDGE_TIMEOUT", "350"))
RECOVER_WINDOW = int(os.getenv("DIAG_RECOVER", "300"))
MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}


def arm_chaos(enabled):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={INTERVAL} "
        f"CHAOS_KILLS_MIN={KILLS} CHAOS_KILLS_MAX={KILLS} "
        f"CHAOS_START_DELAY=3 CHAOS_SEED={SEED} "
        f'CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def snapshot(expected, label):
    down = sorted(set(expected) - running_names())
    print(f"  [{label}] queue={queue_total()}  down={len(down)}/{len(expected)}: {down}", flush=True)
    return down


def watch_recovery(expected):
    """Chaos is disarmed. Watch whether the client finishes and the queue drains."""
    start = time.time()
    while clients_running():
        time.sleep(5)
        elapsed = int(time.time() - start)
        down = sorted(set(expected) - running_names())
        print(f"  recover t={elapsed}s  queue={queue_total()}  down={len(down)}", flush=True)
        if elapsed > RECOVER_WINDOW:
            return False
    return True


def _backlog_queues():
    raw = out("docker exec rabbitmq rabbitmqctl list_queues name messages 2>&1")
    rows = []
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit() and int(parts[1]) > 0:
            rows.append((parts[0], int(parts[1])))
    return rows


def dump_deadlock(down):
    print("\n  === DEADLOCK diagnostics ===", flush=True)
    backlog = _backlog_queues()
    print(f"  --- queues with backlog: {backlog} ---", flush=True)
    # the stuck stage is whichever ring queue still holds a token; dump BOTH peers to find the one that never completed.
    stages = sorted({q.split("_ring_queue")[0] for q, _ in backlog if "_ring_queue" in q})
    for stage in stages:
        for peer in (f"{stage}_0", f"{stage}_1"):
            print(f"  --- {peer} tail (INFO) ---", flush=True)
            print(out(f"docker logs --tail 50 {peer} 2>&1"), flush=True)
    print("  --- supervisor tail ---", flush=True)
    print(out("docker logs --tail 25 supervisor 2>&1"), flush=True)


def main():
    print(f"setup: {TIER}, min2, checkpoint_every={CHECKPOINT}, chaos i{INTERVAL} k{KILLS} seed{SEED}",
          flush=True)
    setup_tier(TIER)
    write_topology(MIN2)
    bring_up_cluster(do_build=True, checkpoint_every=CHECKPOINT)  # rebuild to pick up the ring fix
    expected = expected_controllers()
    print(f"cluster up: {len(expected)} controllers\n", flush=True)

    clear_responses()
    arm_chaos(True)
    recreate_client()
    start = time.time()
    wedged = False
    while clients_running():
        time.sleep(5)
        elapsed = int(time.time() - start)
        down = sorted(set(expected) - running_names())
        print(f"  run t={elapsed}s  queue={queue_total()}  down={len(down)}", flush=True)
        if elapsed > WEDGE_TIMEOUT:
            wedged = True
            break

    if not wedged:
        print(f"\nclient finished in {int(time.time() - start)}s — NO wedge this run "
              f"(i2 is probabilistic; re-run to catch it).", flush=True)
        return

    print(f"\nWEDGED after {int(time.time() - start)}s. Disarming chaos and watching recovery ...",
          flush=True)
    down_at_wedge = snapshot(expected, "wedge")
    arm_chaos(False)  # stop killing; supervisor keeps reviving, no new kills
    recovered = watch_recovery(expected)

    if recovered:
        print("\n  VERDICT: LIVELOCK / recovery-can't-keep-up.", flush=True)
        print("  The client FINISHED once chaos stopped. The system self-heals the moment", flush=True)
        print("  kill pressure is removed -> i2 is a real throughput limit, NOT a deadlock bug.",
              flush=True)
        print("  The cliff is legitimate to document: sustained 120 kills/min outpaces recovery.",
              flush=True)
    else:
        print("\n  VERDICT: DEADLOCK.", flush=True)
        print("  Chaos stopped but the client still did NOT finish within "
              f"{RECOVER_WINDOW}s. Stuck state -> real bug.", flush=True)
        dump_deadlock(down_at_wedge)
        print("\n  Cluster left UP for further inspection.", flush=True)


if __name__ == "__main__":
    main()
