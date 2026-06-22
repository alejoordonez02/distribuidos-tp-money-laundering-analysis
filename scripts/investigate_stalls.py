"""Overnight stall investigation on small/min2/checkpoint-1000.

PROBE A (borderline): run small under chaos at interval=2s (~120 nodes/min) with several
seeds. If some complete and some wedge, the cliff is probabilistic, not a hard line — which
explains why small completed at 2s once while medium collapsed.

PROBE B (transient wedge): run small under chaos at interval=20s (the cadence that wedged
once but completes at faster cadences) with several seeds, watching for a wedge. If one is
caught, dump diagnostics (down nodes, supervisor log tail, queue) and stop with the cluster
up for inspection.

Each trial gets a fresh cluster so trials are independent.

    PYTHONPATH=src uv run scripts/investigate_stalls.py
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
    out,
    queue_total,
    recreate_client,
    run,
    running_names,
    setup_tier,
    teardown,
    write_topology,
)

K = 4
CHECKPOINT = 1000
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}


def arm(interval, seed):
    env = (
        f"CHAOS_ENABLED=1 CHAOS_INTERVAL={interval} "
        f"CHAOS_KILLS_MIN={K} CHAOS_KILLS_MAX={K} CHAOS_START_DELAY=3 "
        f'CHAOS_SEED={seed} CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def fresh_trial(interval, seed, timeout):
    """Fresh cluster, arm chaos, one client. Returns (elapsed, completed, expected)."""
    bring_up_cluster(do_build=False, checkpoint_every=CHECKPOINT)
    expected = expected_controllers()
    clear_responses()
    arm(interval, seed)
    recreate_client()
    start = time.time()
    while clients_running():
        time.sleep(5)
        if time.time() - start > timeout:
            return int(time.time() - start), False, expected
    return int(time.time() - start), True, expected


def dump(expected):
    down = sorted(set(expected) - running_names())
    print(f"  DOWN ({len(down)}/{len(expected)}): {down}", flush=True)
    print(f"  queue total: {queue_total()}", flush=True)
    print("  --- supervisor tail ---", flush=True)
    print(out("docker logs --tail 30 supervisor 2>&1"), flush=True)


def main():
    setup_tier("small")
    write_topology(MIN2)

    print("=== PROBE A: borderline @ 2s (~120 nodos/min), seeds 1234-1237 ===", flush=True)
    a_results = []
    for seed in (1234, 1235, 1236, 1237):
        elapsed, done, _ = fresh_trial(2, seed, timeout=300)
        a_results.append((seed, elapsed, done))
        print(f"  seed {seed}: {elapsed}s -> {'COMPLETED' if done else 'WEDGED'}", flush=True)
    completed = sum(1 for _, _, d in a_results if d)
    print(f"  PROBE A: {completed}/4 completed at 2s "
          f"({'borderline/probabilistic' if 0 < completed < 4 else 'consistent'})", flush=True)

    print("\n=== PROBE B: transient wedge hunt @ 20s, seeds 1234-1240 ===", flush=True)
    for seed in (1234, 1235, 1236, 1237, 1238, 1239, 1240):
        elapsed, done, expected = fresh_trial(20, seed, timeout=150)
        print(f"  seed {seed}: {elapsed}s -> {'completed' if done else 'WEDGED'}", flush=True)
        if not done:
            print(f"\n  >>> caught a wedge at 20s seed {seed}! diagnostics:", flush=True)
            dump(expected)
            print("\n  cluster left UP for inspection (docker ps / docker logs).", flush=True)
            return
    print("\nno 20s wedge caught in 7 seeds — confirms it is rare/intermittent.", flush=True)
    teardown()


if __name__ == "__main__":
    main()
