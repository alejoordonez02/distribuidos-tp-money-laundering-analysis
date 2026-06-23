"""Re-run, in isolation, the chaos configs that ended validated_5_5=False in the full
ft-perf bench, to tell flaky from a real edge. Each config gets a fresh cluster + clean
state and the SAME chaos seed (1234) the bench used. One measured attempt:

  * completes + 5/5  -> flaky under extreme chaos, mark correct;
  * completes + !5/5 -> dump a VERBOSE per-UC pytest diff to diagnose;
  * never completes  -> a cliff (note it).

Does NOT touch results.csv (it is edited by hand afterwards). Reuses the built images.

    PYTHONPATH=src uv run scripts/rerun_failed.py [name,name,...]

Names: small-k24 medium-i20 medium-i1 large-k16 large-k24 (default: all).
"""

import os
import sys
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

# name -> (tier, interval, kills, cap_s)   [checkpoint_every=1000, topology min2]
CONFIGS = {
    "small-k24":  ("small",  20, 24, 900),    # orig UC3
    "medium-i20": ("medium", 20, 4,  1800),   # orig UC3
    "medium-i1":  ("medium", 1,  4,  3600),   # orig UC3
    "large-k16":  ("large",  20, 16, 5400),   # orig UC1
    "large-k24":  ("large",  20, 24, 5400),   # orig UC1
}


def clean_state():
    run(f'docker run --rm -v "{os.getcwd()}/state:/state" alpine '
        "find /state -mindepth 1 -delete", capture=True)


def arm_chaos(enabled, interval, kills):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={interval} "
        f"CHAOS_KILLS_MIN={kills} CHAOS_KILLS_MAX={kills} CHAOS_START_DELAY=3 "
        f'CHAOS_SEED={SEED} CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def verbose_verify():
    r = run(
        "uv run pytest test/test_uc1.py test/test_uc2.py test/test_uc3.py "
        "test/test_uc4.py test/test_uc5.py -v --no-header -rN",
        capture=True,
    )
    return r.stdout or ""


def main():
    which = sys.argv[1].split(",") if len(sys.argv) > 1 else list(CONFIGS)
    summary = []
    first = True
    for name in which:
        tier, interval, kills, cap = CONFIGS[name]
        print(f"\n===== RE-RUN {name}  (tier={tier} i{interval} k{kills} seed={SEED} cap={cap}s) =====", flush=True)
        teardown()
        clean_state()
        setup_tier(tier)
        write_topology(MIN2)
        bring_up_cluster(do_build=first, checkpoint_every=1000)
        first = False

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

        if not completed:
            print(f"RESULT {name}: completed=False (cliff at cap {cap}s)", flush=True)
            summary.append((name, "CLIFF", f"{total}s"))
            wait_drain(180)
            continue

        ok, tail = verify()
        verdict = "PASS 5/5 (flaky -> correct)" if ok else f"FAIL: {tail}"
        print(f"RESULT {name}: completed=True validated={ok} total_s={total} -> {verdict}", flush=True)
        summary.append((name, "PASS" if ok else "FAIL", f"{total}s"))
        if not ok:
            print(f"--- VERBOSE DIAGNOSIS {name} ---", flush=True)
            print(verbose_verify()[-4000:], flush=True)
        wait_drain(180)

    teardown()
    print("\n========== RE-RUN SUMMARY ==========", flush=True)
    for name, st, info in summary:
        print(f"  {name:<12} {st:<6} {info}", flush=True)


if __name__ == "__main__":
    main()
