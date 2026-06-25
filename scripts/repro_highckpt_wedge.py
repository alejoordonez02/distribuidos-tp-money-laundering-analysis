"""Reproduce, in isolation, the HIGH-checkpoint chaos DEADLOCK (deterministic).

Unlike scripts/repro_wedge.py (small/c1000 intermittent timing wedge), this one targets the
REPRODUCIBLE deadlock that appears only when checkpoint_every is very high under chaos:

    tier=medium  topology=min2  checkpoint_every=16000
    CHAOS_SEED=1234  CHAOS_INTERVAL=10s  CHAOS_KILLS=4 per wave

Confirmed twice on F3: ~1810s, validated=False, only 1-3 workers down + ~1M msgs stuck in
queue. The hypothesis: a SIGKILL'd node loses unconfirmed ring/output state (the EOF-ring
control channel in MultiQueueConsumer.send publishes WITHOUT confirm_delivery), so a
downstream barrier waits forever (expected > received). High checkpoint_every widens the
window of unpersisted work lost per kill.

Two things make this a FAST reproducer despite the full bench taking ~30 min:
  1. Early STALL detector: instead of waiting out the 1810s timeout cap, it declares the
     wedge as soon as the queue stops draining (no new low for STALL_WINDOW seconds) while
     the client is still running and only a handful of nodes are down — typically minutes.
  2. Everything is env-overridable, so you can try the even-faster candidate:
        TIER=small CHECKPOINT=16000   (small never reaches 16000 msgs → one kill loses
        essentially all output → should wedge in a couple of minutes if the mechanism holds)

Usage (confirmed config):
    PYTHONPATH=src uv run scripts/repro_highckpt_wedge.py

Fast candidate:
    PYTHONPATH=src TIER=small CHECKPOINT=16000 STALL_WINDOW=90 uv run scripts/repro_highckpt_wedge.py

On wedge it STOPS and leaves the stuck cluster UP for inspection:
    docker exec rabbitmq rabbitmqctl list_queues name messages | sort -k2 -n | tail
    docker logs <stuck-node>          # the node(s) printed as "down" or its revived peer
    docker ps -a

Do NOT run while the benchmark (make performance_vs_ft) is running — they share the host.
Cleanup when done:
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
    queue_total,
    recreate_client,
    run,
    running_names,
    setup_tier,
    write_topology,
)


def _int(name, default):
    return int(os.getenv(name, str(default)))


SEED = _int("CHAOS_SEED", 1234)
INTERVAL = _int("INTERVAL", 10)
KILLS = _int("KILLS", 4)
CHECKPOINT = _int("CHECKPOINT", 16000)
TIER = os.getenv("TIER", "medium")
EXCLUDE = os.getenv("CHAOS_EXCLUDE", "rabbitmq,supervisor,gateway,chaos")

# A hard ceiling so the script never runs longer than the bench would (matches the 1810s cap).
HARD_TIMEOUT = _int("HARD_TIMEOUT", 1900)
# Early stall verdict: queue makes no new low for this long while the client runs -> wedged (kept above a legit recovery plateau).
STALL_WINDOW = _int("STALL_WINDOW", 150)
# Don't arm the stall detector until the pipeline has had a chance to ramp up and drain once.
MIN_RUNTIME = _int("MIN_RUNTIME", 120)
POLL = _int("POLL", 5)

MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}


def arm_chaos(enabled):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={INTERVAL} "
        f"CHAOS_KILLS_MIN={KILLS} CHAOS_KILLS_MAX={KILLS} "
        f"CHAOS_START_DELAY=3 CHAOS_SEED={SEED} "
        f'CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def _verdict(expected):
    """Run one chaos client and watch the queue. Return (elapsed, status) where status is
    'completed', 'wedged', or 'timeout'."""
    clear_responses()
    arm_chaos(True)
    recreate_client()
    start = time.time()
    best_queue = None          # lowest queue depth seen so far (after it first appears)
    best_at = start            # when we last saw a new low — the drain progress clock
    while clients_running():
        time.sleep(POLL)
        elapsed = int(time.time() - start)
        q = queue_total()
        down = sorted(set(expected) - running_names())
        stuck_for = int(time.time() - best_at)
        print(f"  t={elapsed}s  queue={q}  down={len(down)}{' '+str(down) if down else ''}  "
              f"no-drain-progress={stuck_for}s")

        if q is not None and (best_queue is None or q < best_queue):
            best_queue, best_at = q, time.time()

        # patological stall: client alive, past warm-up, queue not draining, cluster mostly up
        if (elapsed > MIN_RUNTIME and stuck_for > STALL_WINDOW
                and (q is None or q > 0) and len(down) <= max(4, KILLS)):
            return elapsed, "wedged"
        if elapsed > HARD_TIMEOUT:
            return elapsed, "timeout"
    return int(time.time() - start), "completed"


def main():
    print(f"setup: tier={TIER} min2 checkpoint_every={CHECKPOINT}  "
          f"chaos seed={SEED} interval={INTERVAL}s kills={KILLS}/wave")
    print(f"stall detector: declare wedged after {STALL_WINDOW}s with no queue drain "
          f"(warm-up {MIN_RUNTIME}s, hard cap {HARD_TIMEOUT}s)")
    setup_tier(TIER)
    write_topology(MIN2)
    bring_up_cluster(do_build=_int("BUILD", 0) == 1, checkpoint_every=CHECKPOINT)
    expected = expected_controllers()
    print(f"cluster up: {len(expected)} controllers\n")

    elapsed, status = _verdict(expected)
    if status == "completed":
        print(f"\nCOMPLETED in {elapsed}s — no wedge with this config. "
              f"If you expected a wedge, bump CHECKPOINT or lower INTERVAL.")
        return
    why = "hard timeout" if status == "timeout" else "early stall detector"
    print(f"\nWEDGED after {elapsed}s ({why}). Cluster left UP for inspection:")
    print("  docker exec rabbitmq rabbitmqctl list_queues name messages | sort -k2 -n | tail")
    print("  docker logs <stuck-node>   # the 'down' node above, or its surviving ring peer")
    print("  docker ps -a")


if __name__ == "__main__":
    main()
