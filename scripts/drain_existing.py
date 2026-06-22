"""Disarm chaos on the ALREADY-RUNNING cluster and wait: if the client drains and
finishes, the stall was a CLIFF (liveness under heavy chaos, not a broken pipeline);
if it stays stuck with chaos off, something is deadlocked. Does NOT bring up a fresh
cluster — operates on whatever is currently up.

    PYTHONPATH=src uv run scripts/drain_existing.py
"""

import time

from ft_common import COMPOSE, clients_running, run, verify

INTERVAL = 20
KILLS = 24
SEED = 1234
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
DRAIN_CAP = 1800


def arm_chaos(enabled):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={INTERVAL} "
        f"CHAOS_KILLS_MIN={KILLS} CHAOS_KILLS_MAX={KILLS} CHAOS_START_DELAY=3 "
        f'CHAOS_SEED={SEED} CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def main():
    if not clients_running():
        print("no client running — nothing to drain (it may have already finished)", flush=True)
    else:
        print("DISARMING chaos on the running cluster, waiting for drain...", flush=True)
        arm_chaos(False)
        start = time.time()
        drained = False
        while clients_running():
            time.sleep(5)
            if time.time() - start > DRAIN_CAP:
                break
        else:
            drained = True
        if drained:
            print(f">>> DRAINED in {int(time.time() - start)}s after disarming = CLIFF (fix not broken)", flush=True)
        else:
            print(">>> STILL STUCK with chaos off = BROKEN (deadlock) — cluster left up", flush=True)
            return

    print("verifying 5/5...", flush=True)
    ok, detail = verify()
    print(f">>> VALIDATION: {'PASS 5/5' if ok else 'FAIL'} :: {detail}", flush=True)


if __name__ == "__main__":
    main()
