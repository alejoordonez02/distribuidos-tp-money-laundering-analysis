"""Re-run, in isolation, the two chaos configs that validated 4/5 in the full bench
(F1 small i2 freq, and F2 medium k24 burst), each until it COMPLETES, then run the full
5/5 oracle verify. Prints one RESULT line per config so the CSV can be updated.

    PYTHONPATH=src uv run scripts/rerun_failed.py

Reuses the already-built image (no rebuild). Leaves the cluster torn down at the end.
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
    recreate_client,
    run,
    setup_tier,
    teardown,
    verify,
    wait_drain,
    write_topology,
)

MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}
SEED = 1234
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
MAX_ATTEMPTS = 4

# (label, run_id, tier, interval, kills, cap_s)
CONFIGS = [
    ("F1 small i2 freq", "F1-small-min2-c1000-chaos-i2-k4", "small", 2, 4, 700),
    ("F2 medium k24 burst", "F2-medium-min2-c1000-chaos-i20-k24", "medium", 20, 24, 3600),
]


def arm_chaos(enabled, interval, kills):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={interval} "
        f"CHAOS_KILLS_MIN={kills} CHAOS_KILLS_MAX={kills} CHAOS_START_DELAY=3 "
        f'CHAOS_SEED={SEED} CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def run_until_complete(interval, kills, cap):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        clear_responses()
        arm_chaos(True, interval, kills)
        start = time.time()
        recreate_client()
        completed = True
        while clients_running():
            time.sleep(5)
            if time.time() - start > cap:
                completed = False
                break
        total = int(time.time() - start)
        arm_chaos(False, interval, kills)
        if completed:
            return attempt, total
        print(f"    attempt {attempt}: did not finish in {cap}s (cliff) — retrying", flush=True)
        wait_drain(180)
    return None, None


def main():
    rounds = int(os.getenv("ROUNDS", "3"))
    first = True
    summary = {}
    for r in range(1, rounds + 1):
        for label, run_id, tier, interval, kills, cap in CONFIGS:
            print(f"\n=== round {r}/{rounds}  {label}  ({run_id}) ===", flush=True)
            setup_tier(tier)
            write_topology(MIN2)
            bring_up_cluster(do_build=first, checkpoint_every=1000)
            first = False
            attempt, total = run_until_complete(interval, kills, cap)
            if total is None:
                print(f"RESULT round={r} run_id={run_id} completed=False validated=False detail=cliff", flush=True)
                summary.setdefault(run_id, []).append("cliff")
                continue
            ok, tail = verify()
            print(f"RESULT round={r} run_id={run_id} completed=True validated={ok} total_s={total} detail={tail!r}", flush=True)
            summary.setdefault(run_id, []).append(f"PASS:{total}" if ok else "FAIL")
    print("\n=== SUMMARY ===", flush=True)
    for rid, outcomes in summary.items():
        print(f"{rid}: {outcomes}", flush=True)
    teardown()
    teardown()


if __name__ == "__main__":
    main()
