"""Localize the UC3 transaction loss under medium-k24 burst: run it, then dump the
per-stage CNTDBG sent counts + the join received count so we can see where the count drops.

    PYTHONPATH=src uv run scripts/diag_uc3_counts.py

Leaves the cluster UP for inspection. Rebuilds to pick up the CNTDBG instrumentation.
"""

import time

from ft_common import (
    COMPOSE,
    CURRENT,
    TOPOLOGY_KEYS,
    bring_up_cluster,
    clear_responses,
    clients_running,
    out,
    recreate_client,
    run,
    setup_tier,
    verify,
    wait_drain,
    write_topology,
)

MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}
SEED = 1234
INTERVAL = 20
KILLS = 24
CAP = 2400
MAX_ATTEMPTS = 4
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"


def arm_chaos(enabled):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={INTERVAL} "
        f"CHAOS_KILLS_MIN={KILLS} CHAOS_KILLS_MAX={KILLS} CHAOS_START_DELAY=3 "
        f'CHAOS_SEED={SEED} CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def run_once():
    for attempt in range(1, MAX_ATTEMPTS + 1):
        clear_responses()
        arm_chaos(True)
        start = time.time()
        recreate_client()
        while clients_running():
            time.sleep(5)
            if time.time() - start > CAP:
                break
        else:
            arm_chaos(False)
            return int(time.time() - start)
        arm_chaos(False)
        print(f"  attempt {attempt}: cliff, retrying", flush=True)
        wait_drain(180)
    return None


def dump_counts():
    names = out('docker ps -a --format "{{.Names}}"').split()
    uc3 = [n for n in names if n.startswith(("default_filter", "uc3_", "join_"))]
    print("=== CNTDBG flow (sent per stage + join received) ===", flush=True)
    for n in sorted(uc3):
        logs = out(f"docker logs {n} 2>&1 | grep -E 'RESTOREDBG|JOINDEDUP|SEQDBG|CNTDBG'")
        if logs.strip():
            print(f"--- {n} ---\n{logs}", flush=True)


MAX_TRIALS = 6


def main():
    setup_tier("medium")
    write_topology(MIN2)
    bring_up_cluster(do_build=True, checkpoint_every=1000)
    for n in range(1, MAX_TRIALS + 1):
        print(f"\n--- trial {n}/{MAX_TRIALS} ---", flush=True)
        t = run_once()
        if t is None:
            print(f"  trial {n}: cliff (no completó)", flush=True)
            wait_drain(180)
            continue
        ok, tail = verify()
        print(f"  trial {n}: finished {t}s  verify ok={ok}  {tail!r}", flush=True)
        if not ok:
            print("  *** FALLA GENUINA cazada — dump de conteos ***", flush=True)
            dump_counts()
            print("  cluster left UP for inspection.", flush=True)
            return
        wait_drain(180)
    print("no falló en esta tanda; re-correr.", flush=True)


if __name__ == "__main__":
    main()
