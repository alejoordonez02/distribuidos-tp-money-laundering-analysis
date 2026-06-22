"""Distinguish a CLIFF (chaos-induced livelock, recoverable) from a BROKEN pipeline
(real deadlock) on medium-k24: run under chaos; if the client does not finish by
SOFT_CAP, DISARM the chaos and keep waiting. If it then drains and finishes, it was a
cliff (the fix is correct, k24 just saturates it). If it stays stuck with chaos off,
something is broken.

    PYTHONPATH=src uv run scripts/repro_recovery.py

Leaves the cluster UP for inspection.
"""

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
    verify,
    write_topology,
)

TIER = "medium"
SEED = 1234
INTERVAL = 20
KILLS = 24
CHECKPOINT = 1000
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
SOFT_CAP = 2600
DRAIN_CAP = 2000
MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}


def arm_chaos(enabled):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={INTERVAL} "
        f"CHAOS_KILLS_MIN={KILLS} CHAOS_KILLS_MAX={KILLS} CHAOS_START_DELAY=3 "
        f'CHAOS_SEED={SEED} CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def wait_for_client(cap):
    start = time.time()
    while clients_running():
        time.sleep(5)
        if time.time() - start > cap:
            return False
    return True


def main():
    setup_tier(TIER)
    write_topology(MIN2)
    bring_up_cluster(do_build=False, checkpoint_every=CHECKPOINT)
    clear_responses()
    arm_chaos(True)
    recreate_client()
    print(f"running medium-k24 under chaos (soft cap {SOFT_CAP}s)", flush=True)

    if wait_for_client(SOFT_CAP):
        print("finished UNDER CHAOS (no cliff)", flush=True)
    else:
        print(f"did not finish in {SOFT_CAP}s -> DISARMING CHAOS, waiting to drain", flush=True)
        arm_chaos(False)
        if wait_for_client(DRAIN_CAP):
            print(">>> RECOVERED after disarming chaos = it was a CLIFF (fix not broken)", flush=True)
        else:
            print(">>> STILL STUCK with chaos off = BROKEN (real deadlock) — cluster left up", flush=True)
            return

    print("verifying 5/5...", flush=True)
    ok, detail = verify()
    print(f">>> VALIDATION: {'PASS 5/5' if ok else 'FAIL'} :: {detail}", flush=True)


if __name__ == "__main__":
    main()
