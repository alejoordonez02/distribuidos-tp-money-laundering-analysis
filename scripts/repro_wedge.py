"""Catch, in isolation, the small/min2/checkpoint-1000 chaos wedge.

Under sustained chaos the small pipeline OCCASIONALLY wedges (runs forever instead of
finishing in ~115s). It is INTERMITTENT, not deterministic: the seed fixes which nodes die,
but not the system timing (docker, restore speed, scheduling), so the same kill sequence
wedges on one run and completes on the next. Re-running with the same seed completed fine,
so this is a rare timing interaction worth debugging.

This brings the cluster up once and runs clients in a loop under chaos until one wedges
(exceeds WEDGE_TIMEOUT). When it catches one, it stops and leaves the stuck cluster UP so it
can be inspected (docker ps, docker logs, queue depth, which controllers are down).

    tier=small  topology=min2  checkpoint_every=1000
    CHAOS_SEED=1234  CHAOS_INTERVAL=20s  CHAOS_KILLS=4 per wave

    PYTHONPATH=src uv run scripts/repro_wedge.py

Do NOT run while the benchmark (make performance_vs_ft) is running — they share the host.
Cleanup when done:
    docker compose -f docker-compose.yaml down --remove-orphans
    git checkout scripts/cfg.py scripts/gen_compose/src/topology.py
"""

import time

from ft_common import (
    COMPOSE,
    CURRENT,
    TOPOLOGY_KEYS,
    bring_up_cluster,
    clear_responses,
    clients_running,
    expected_controllers,
    queue_total,
    recreate_client,
    run,
    running_names,
    setup_tier,
    wait_drain,
    write_topology,
)

SEED = 1234
INTERVAL = 20
KILLS = 4
CHECKPOINT = 1000
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
WEDGE_TIMEOUT = 250  # a healthy run finishes ~115s; past this it is wedged
MAX_TRIALS = 10
MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}


def arm_chaos(enabled):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={INTERVAL} "
        f"CHAOS_KILLS_MIN={KILLS} CHAOS_KILLS_MAX={KILLS} "
        f"CHAOS_START_DELAY=3 CHAOS_SEED={SEED} "
        f'CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def run_trial(expected, n):
    clear_responses()
    arm_chaos(True)
    recreate_client()
    start = time.time()
    while clients_running():
        time.sleep(5)
        elapsed = int(time.time() - start)
        down = sorted(set(expected) - running_names())
        print(f"  trial {n}  t={elapsed}s  queue={queue_total()}  down={len(down)}")
        if elapsed > WEDGE_TIMEOUT:
            return elapsed, True
    return int(time.time() - start), False


def main():
    print(f"setup: small, min2, checkpoint_every={CHECKPOINT}")
    setup_tier("small")
    write_topology(MIN2)
    bring_up_cluster(do_build=False, checkpoint_every=CHECKPOINT)
    expected = expected_controllers()
    print(f"cluster up: {len(expected)} controllers — chaos seed={SEED} "
          f"interval={INTERVAL}s kills={KILLS}/wave")

    for n in range(1, MAX_TRIALS + 1):
        print(f"--- trial {n}/{MAX_TRIALS} ---")
        elapsed, wedged = run_trial(expected, n)
        if wedged:
            print(f"\nWEDGED on trial {n} after {elapsed}s (>{WEDGE_TIMEOUT}s).")
            print("Cluster left UP for inspection. Look at:")
            print("  docker ps -a   |   docker logs supervisor   |   docker logs <stuck-node>")
            print("  docker exec rabbitmq rabbitmqctl list_queues name messages")
            return
        print(f"trial {n} completed in {elapsed}s; disarming chaos and draining ...")
        arm_chaos(False)
        wait_drain(120)
    print(f"\nno wedge in {MAX_TRIALS} trials — it is intermittent, just run it again.")


if __name__ == "__main__":
    main()
