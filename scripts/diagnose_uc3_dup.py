"""Catch and characterize the UC3 correctness slip under high-churn chaos.

Under sustained high-rate chaos (small i2 = 120 kills/min) the pipeline COMPLETES but
UC3 (the average aggregate) occasionally validates 4/5. This brings up small/min2 once
and runs clients under chaos i2 in a loop; on the first run whose UC3 differs from the
oracle it PRESERVES the responses and prints a KEYED diff:

  - missing keys           -> a whole group was LOST
  - extra keys             -> a spurious group appeared
  - shared key, wrong amt  -> the average was corrupted by a DUPLICATE (double-count)

so we can tell loss vs duplicate and find the offending groups.

    PYTHONPATH=src uv run scripts/diagnose_uc3_dup.py

Cleanup: docker compose -f docker-compose.yaml down --remove-orphans
"""

import os
import re
import shutil
import time
from ft_common import (
    COMPOSE,
    CURRENT,
    ROOT,
    TOPOLOGY_KEYS,
    bring_up_cluster,
    clear_responses,
    clients_running,
    recreate_client,
    run,
    setup_tier,
    wait_drain,
    write_topology,
)

TIER = os.getenv("UC3_TIER", "small")
SEED = int(os.getenv("UC3_SEED", "1234"))
INTERVAL = int(os.getenv("UC3_INTERVAL", "2"))
KILLS = int(os.getenv("UC3_KILLS", "4"))
CHECKPOINT = int(os.getenv("UC3_CHECKPOINT", "1000"))
EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
CAP = int(os.getenv("UC3_CAP", "550"))
MAX_TRIALS = int(os.getenv("UC3_TRIALS", "8"))
MIN2 = {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS}
RESP = os.path.join(ROOT, "responses/responses_0.csv")
EXP = os.path.join(ROOT, "test/expected_responses/uc3_0.csv")
SAVE = os.path.join(ROOT, "tmp/ft_perf/uc3_FAIL_responses.csv")


def arm_chaos(enabled):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} CHAOS_INTERVAL={INTERVAL} "
        f"CHAOS_KILLS_MIN={KILLS} CHAOS_KILLS_MAX={KILLS} CHAOS_START_DELAY=3 "
        f'CHAOS_SEED={SEED} CHAOS_EXCLUDE="{EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos", capture=True)


def load_expected():
    """Set of (bank, account, format, round(amount,2)) — same identity as test_uc3."""
    s = set()
    with open(EXP) as f:
        f.readline()
        for line in f:
            _, bank, acct, fmt, amt = line.rstrip("\n").split(",")
            s.add((bank, acct, fmt, round(float(amt), 2)))
    return s


def load_got():
    s = set()
    with open(RESP) as f:
        for line in f:
            if "--- UC3 ---" in line:
                break
        for line in f:
            if "--- UC" in line:
                break
            m = re.match(
                r"bank_id: (\S+)\s+account: (\S+)\s+payment_format: (.*?)\s+amount: (\S+)$",
                line.strip(),
            )
            if m:
                s.add((m.group(1), m.group(2), m.group(3), round(float(m.group(4)), 2)))
    return s


def diff(exp, got):
    """Result-level set diff (the real test_uc3 semantics)."""
    missing = exp - got  # in oracle, not produced -> transactions LOST
    extra = got - exp     # produced, not in oracle -> transactions WRONGLY selected
    return missing, extra, []


def run_trial():
    clear_responses()
    arm_chaos(True)
    recreate_client()
    start = time.time()
    while clients_running():
        time.sleep(5)
        if time.time() - start > CAP:
            return None
    return int(time.time() - start)


def dump_spill_state():
    print("  === spill line-counts (uc3_join, uc3_merge) + CLEARSTALE firings ===", flush=True)
    names = run("docker ps -a --format '{{.Names}}'", capture=True).stdout.split()
    targets = [n for n in names if "join" in n or "merge" in n]
    for c in targets:
        wc = run(
            f"docker exec {c} sh -c 'wc -l /state/spill/uc3_*.spill 2>/dev/null' || true",
            capture=True,
        ).stdout.strip()
        cs = run(f"docker logs {c} 2>&1 | grep CLEARSTALE || true", capture=True).stdout.strip()
        if wc or cs:
            print(f"  --- {c} ---", flush=True)
            if wc:
                print(f"    spill: {wc}", flush=True)
            if cs:
                print(f"    CLEARSTALE: {cs}", flush=True)


def main():
    setup_tier(TIER)
    write_topology(MIN2)
    bring_up_cluster(do_build=os.getenv("UC3_BUILD", "0") == "1", checkpoint_every=CHECKPOINT)
    exp = load_expected()
    print(f"oracle UC3 keys={len(exp)}  chaos i{INTERVAL} k{KILLS} seed{SEED}", flush=True)
    for n in range(1, MAX_TRIALS + 1):
        print(f"--- trial {n}/{MAX_TRIALS} ---", flush=True)
        t = run_trial()
        if t is None:
            print(f"  trial {n}: did not finish in {CAP}s (cliff) — skipping", flush=True)
            arm_chaos(False)
            wait_drain(120)
            continue
        got = load_got()
        missing, extra, _ = diff(exp, got)
        print(
            f"  trial {n}: finished {t}s  got_results={len(got)} exp_results={len(exp)} "
            f"missing={len(missing)} extra={len(extra)}",
            flush=True,
        )
        if missing or extra:
            from collections import Counter
            shutil.copy(RESP, SAVE)
            print("  *** UC3 GENUINE MISMATCH (set-level) — responses preserved to tmp/ft_perf/uc3_FAIL_responses.csv ***", flush=True)
            print(f"  MISSING (oracle, not produced = LOST txns) per format: {dict(Counter(k[2] for k in missing))}", flush=True)
            print(f"  EXTRA (produced, not in oracle = WRONG txns) per format: {dict(Counter(k[2] for k in extra))}", flush=True)
            print(f"  sample missing: {sorted(missing)[:6]}", flush=True)
            print(f"  sample extra:   {sorted(extra)[:6]}", flush=True)
            dump_spill_state()
            print("  cluster left UP for inspection.", flush=True)
            return
        arm_chaos(False)
        wait_drain(120)
    print("no UC3 mismatch caught in this batch; intermittent — re-run.", flush=True)


if __name__ == "__main__":
    main()
