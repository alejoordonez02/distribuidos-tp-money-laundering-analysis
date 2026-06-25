"""Fault-tolerance vs performance benchmark (`make performance_vs_ft`).

Measures how the chaos monkey (sustained node-killing) degrades pipeline throughput, and how
that interacts with the checkpoint frequency. Produces a CSV that plot_ft_perf.py turns into
the report figures.

Shares the cluster-driving and topology/tier helpers with the other e2e tests via
ft_common.py: the cluster is brought up once per (tier, topology, checkpoint_every) and many
fresh clients run through it — only the chaos container is re-armed between runs, the
controllers stay warm. Stalls are detected by progress, with a fast per-node wedge check so a
killed node the supervisor never revives is caught in minutes, not at the cap.

Three regimes, one variable: chaos OFF (the performance reference) vs chaos ON. There is no
"without fault tolerance" — checkpointing + dedup are intrinsic.

Two orthogonal slices through the chaos stress plane (kill rate = K nodes x 60/interval),
each exposing a different failure mode, plus the checkpoint trade-off:
  F1 frequency sweep — fix the burst K, shrink the wave interval (cumulative: supervisor
                       falls behind over time). X axis = interval in seconds.
  F2 burst sweep     — fix a generous interval, grow the wave size K (simultaneity: one wave
                       takes out a critical mass). X axis = nodes killed per wave.
  F3 checkpoint dual-curve on medium — checkpoint_every helps without chaos, hurts under it.
Each ramp runs across tiers (small, medium, large; large coarser). Select a subset with
FT_PERF_PHASES=F1,... and restrict tiers with FT_PERF_RAMP_TIERS=small,medium.

Every row is appended to tmp/ft_perf/results.csv immediately; rows already present are skipped.
"""

import csv
import os
import re
import sys
import time
from datetime import datetime

from ft_common import (
    COMPOSE,
    CURRENT,
    ROOT,
    TIER_CAP,
    TOPOLOGY_KEYS,
    bring_up_cluster,
    clear_responses,
    client_exit_codes,
    clients_running,
    expected_controllers,
    out,
    queue_total,
    recreate_client,
    restore_files,
    run,
    running_names,
    setup_tier,
    snapshot_files,
    teardown,
    variant,
    verify,
    wait_drain,
    write_topology,
)

RESULTS_DIR = os.path.join(ROOT, "tmp/ft_perf")
RESULTS_CSV = os.path.join(RESULTS_DIR, "results.csv")
STATUS_FILE = os.path.join(RESULTS_DIR, "STATUS.txt")
DEBUG_CASES = os.path.join(RESULTS_DIR, "debug_cases.md")

CSV_FIELDS = [
    "run_id", "ts", "phase", "tier", "topology", "checkpoint_every",
    "chaos_enabled", "chaos_interval", "kills_per_wave",
    "total_s", "validated_5_5", "completed", "recovered",
    "n_kills", "n_revives", "mean_recovery_s", "error",
]

PROGRESS_INTERVAL = int(os.getenv("FT_PERF_PROGRESS", "5"))
STALL_GRACE = int(os.getenv("FT_PERF_STALL_GRACE", "180"))
WEDGE_GRACE = int(os.getenv("FT_PERF_WEDGE_GRACE", "240"))
DRAIN_TIMEOUT = int(os.getenv("FT_PERF_DRAIN", "180"))
RECOVER_WINDOW = os.getenv("FT_PERF_RECOVER_WINDOW")
CHAOS_START_DELAY = int(os.getenv("FT_PERF_CHAOS_START_DELAY", "3"))
CHAOS_EXCLUDE = "rabbitmq,supervisor,gateway,chaos"
CHAOS_SEED_BASE = int(os.getenv("FT_PERF_CHAOS_SEED", "1234"))

TOPOLOGIES = {
    "current": dict(CURRENT),
    "min2": {k: max(2, CURRENT[k]) for k in TOPOLOGY_KEYS},
    "bottleneck3": variant(
        UC4_COUNT_PATHS=3, UC4_PATHS_AGGREGATES=3, UC4_AGGREGATE_GRAPHS=3,
        UC3_AGGREGATES=3, UC2_MAX_AMOUNT_AGGREGATES=2, UC2_BANK_NAMES_AGGREGATES=2,
    ),
    "aggressive": {k: max(3, CURRENT[k]) if k != "UC4_DEGREE_AGGREGATES" else 2
                   for k in TOPOLOGY_KEYS},
}


def _ts(line):
    """Parse a leading RFC3339 timestamp from a `docker logs -t` line -> epoch float."""
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", line)
    if not m:
        return None
    raw = m.group(1)
    if "." in raw:
        raw = raw[:raw.index(".") + 7]
        fmt = "%Y-%m-%dT%H:%M:%S.%f"
    else:
        fmt = "%Y-%m-%dT%H:%M:%S"
    try:
        return datetime.strptime(raw, fmt).timestamp()
    except ValueError:
        return None


def parse_recovery_metrics(chaos_log, supervisor_log):
    """Best-effort recovery stats from timestamped docker logs. total_s is the rock-solid
    metric; these enrich it, degrading to 0 / None when a piece is missing."""
    n_kills = sum(int(m) for m in re.findall(r"KILLED (\d+) node", chaos_log))
    n_revives = len(re.findall(r"reviving (\S+)", supervisor_log))

    deaths: dict[str, list[float]] = {}
    for line in chaos_log.splitlines():
        ts = _ts(line)
        if ts is None or "KILLED" not in line or "->" not in line:
            continue
        for node in re.findall(r"[a-z][a-z0-9_]*_\d+", line.split("->", 1)[1]):
            deaths.setdefault(node, []).append(ts)

    recoveries = []
    for line in supervisor_log.splitlines():
        ts = _ts(line)
        if ts is None:
            continue
        mnode = re.search(r"[a-z][a-z0-9_]*_\d+", line)
        node = mnode.group(0) if mnode else None
        if node and "dead" in line:
            deaths.setdefault(node, []).append(ts)
        elif node and "recovered" in line and "alive" in line:
            recoveries.append((node, ts))

    times = []
    for node, ts in recoveries:
        prior = [d for d in deaths.get(node, []) if d <= ts]
        if prior:
            times.append(ts - max(prior))
    mean_recovery = round(sum(times) / len(times), 2) if times else None
    return {"n_kills": n_kills, "n_revives": n_revives, "mean_recovery_s": mean_recovery}


def arm_chaos(enabled, interval, kills, seed):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} "
        f"CHAOS_INTERVAL={interval} "
        f"CHAOS_KILLS_MIN={kills} CHAOS_KILLS_MAX={kills} "
        f"CHAOS_START_DELAY={CHAOS_START_DELAY} "
        f"CHAOS_SEED={seed} "
        f'CHAOS_EXCLUDE="{CHAOS_EXCLUDE}" '
    )
    run(f"{env} docker compose -f {COMPOSE} up -d --force-recreate --no-deps chaos",
        capture=True)


def wait_for_client(tier, expected):
    """Progress-based completion/stall detection under sustained chaos. A specific controller
    down continuously for WEDGE_GRACE means the supervisor failed to revive it (per-node, so
    heavy in-node compute is never mistaken for a wedge); a queue frozen with the whole
    cluster up for STALL_GRACE is a logical deadlock. An empty queue counts as healthy."""
    cap = TIER_CAP[tier]
    start = time.time()
    last_total = None
    stall_since = time.time()
    down_since: dict[str, float] = {}
    while clients_running():
        time.sleep(PROGRESS_INTERVAL)
        now = time.time()
        if now - start > cap:
            return False
        running = running_names()
        for n in expected:
            if n in running:
                down_since.pop(n, None)
            else:
                down_since.setdefault(n, now)
        if any(now - t > WEDGE_GRACE for t in down_since.values()):
            return False
        total = queue_total()
        if total == 0 or (total is not None and total != last_total):
            last_total, stall_since = total, now
            continue
        if down_since:
            stall_since = now
            continue
        if now - stall_since > STALL_GRACE:
            return False
    return True


def _recover_window(tier):
    return int(RECOVER_WINDOW) if RECOVER_WINDOW else max(300, TIER_CAP[tier] // 2)


def watch_recovery(tier, expected):
    window = _recover_window(tier)
    start = time.time()
    while clients_running():
        time.sleep(PROGRESS_INTERVAL)
        if time.time() - start > window:
            return False
    return True


def record_debug_case(phase, tier, topology, checkpoint, interval, kills, seed, elapsed, expected):
    """Append a stalled run to the debug-cases file so it can be reproduced and investigated
    later. Captured while the cluster is still in its stuck state, so the down nodes and queue
    reflect the wedge."""
    down = sorted(set(expected) - running_names())
    rate = round(kills * 60 / interval) if interval else 0
    block = (
        f"## {datetime.now().isoformat(timespec='seconds')} — {phase} {tier} STALL\n"
        f"- config: tier={tier} topology={topology} checkpoint={checkpoint} "
        f"seed={seed} interval={interval}s kills={kills}/wave (~{rate} nodos/min)\n"
        f"- after {elapsed}s: queue={queue_total()}, {len(down)} controllers down: {down}\n"
        f"- client exit codes: {client_exit_codes()}\n"
        f"- repro: PYTHONPATH=src uv run scripts/repro_wedge.py "
        f"(adjust SEED/INTERVAL/KILLS/CHECKPOINT/tier to match)\n\n"
    )
    with open(DEBUG_CASES, "a") as f:
        f.write(block)
    log(f"  -> STALL logged to debug_cases.md ({len(down)} nodes down)")


def run_once(phase, tier, topology, checkpoint_every, chaos_on, interval, kills, seed):
    """One measured client through the warm cluster. Returns a CSV row dict."""
    expected = expected_controllers()
    clear_responses()
    arm_chaos(chaos_on, interval, kills, seed)

    start = time.time()
    recreate_client()
    completed = wait_for_client(tier, expected)
    total_s = int(time.time() - start)
    if not completed:
        record_debug_case(phase, tier, topology, checkpoint_every, interval, kills, seed, total_s, expected)

    since = f"{total_s + 5}s"
    chaos_log = out(f"docker logs -t --since {since} chaos 2>&1", timeout=20) if chaos_on else ""
    sup_log = out(f"docker logs -t --since {since} supervisor 2>&1", timeout=20)
    metrics = parse_recovery_metrics(chaos_log, sup_log)

    arm_chaos(False, interval, kills, seed)

    recovered: object = ""
    if chaos_on and not completed:
        recovered = watch_recovery(tier, expected)

    validated, detail, error = False, "", ""
    if not completed:
        verdict = "recovered=livelock (legit cliff)" if recovered else "STUCK=deadlock (bug)"
        error = f"STALL/timeout: client did not finish within cap; {verdict}"
    else:
        ok, tail = verify()
        validated, detail = ok, tail
        if not ok:
            error = f"validation != 5/5: {tail}"

    wait_drain(DRAIN_TIMEOUT)

    return {
        "run_id": f"{phase}-{tier}-{topology}-c{checkpoint_every}-{'chaos' if chaos_on else 'base'}-i{interval}-k{kills}",
        "ts": datetime.now().isoformat(timespec="seconds"),
        "phase": phase, "tier": tier, "topology": topology,
        "checkpoint_every": checkpoint_every,
        "chaos_enabled": chaos_on, "chaos_interval": interval, "kills_per_wave": kills,
        "total_s": total_s, "validated_5_5": validated, "completed": completed,
        "recovered": recovered,
        "n_kills": metrics["n_kills"], "n_revives": metrics["n_revives"],
        "mean_recovery_s": metrics["mean_recovery_s"], "error": error,
    }


def load_rows():
    if not os.path.exists(RESULTS_CSV):
        return []
    with open(RESULTS_CSV, newline="") as f:
        return list(csv.DictReader(f))


def append_row(row):
    new = not os.path.exists(RESULTS_CSV)
    with open(RESULTS_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)


def already_done(rows, run_id):
    return any(r.get("run_id") == run_id and str(r.get("completed")) == "True" for r in rows)


def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(STATUS_FILE, "a") as f:
        f.write(line + "\n")


_cluster_dirty = False
_live_cluster = None

def _error_row(run_id, phase, tier, topology, checkpoint, chaos_on, interval, kills, err):
    return {
        "run_id": run_id, "ts": datetime.now().isoformat(timespec="seconds"),
        "phase": phase, "tier": tier, "topology": topology,
        "checkpoint_every": checkpoint, "chaos_enabled": chaos_on,
        "chaos_interval": interval, "kills_per_wave": kills, "total_s": "",
        "validated_5_5": False, "completed": False, "recovered": "",
        "n_kills": "", "n_revives": "", "mean_recovery_s": "", "error": err,
    }


def _ensure_cluster(tier, topology, checkpoint, do_build, force):
    """Bring up a fresh cluster for (tier, topology, checkpoint) only when needed: forced,
    left dirty by a prior failure, or different from what is currently live."""
    global _cluster_dirty, _live_cluster
    key = (tier, topology, checkpoint)
    if force or _cluster_dirty or _live_cluster != key:
        setup_tier(tier)
        write_topology(TOPOLOGIES[topology])
        bring_up_cluster(do_build, checkpoint_every=checkpoint)
        _live_cluster = key
        _cluster_dirty = False


def measure(phase, tier, topology, checkpoint, chaos_on, interval, kills, do_build=False,
            new_cluster=True):
    """Run one measured client, persist a row, and return it. Never raises."""
    global _cluster_dirty, _live_cluster
    run_id = f"{phase}-{tier}-{topology}-c{checkpoint}-{'chaos' if chaos_on else 'base'}-i{interval}-k{kills}"
    rows = load_rows()
    if already_done(rows, run_id):
        log(f"skip (already done): {run_id}")
        return next(r for r in rows if r.get("run_id") == run_id)

    log(f"RUN {run_id}")
    try:
        _ensure_cluster(tier, topology, checkpoint, do_build, force=new_cluster)
        row = run_once(phase, tier, topology, checkpoint, chaos_on, interval, kills, CHAOS_SEED_BASE)
    except Exception as e:
        row = _error_row(run_id, phase, tier, topology, checkpoint, chaos_on, interval, kills,
                         f"exception: {type(e).__name__}: {e}")

    append_row(row)
    if str(row.get("completed")) != "True" or str(row.get("validated_5_5")) != "True":
        _cluster_dirty = True
        _live_cluster = None
    status = "OK 5/5" if str(row.get("validated_5_5")) == "True" else f"PROBLEM: {row.get('error')}"
    log(f"  -> {row.get('total_s')}s  {status}")
    return row


def _ints(env, default):
    return [int(x) for x in os.getenv(env, default).split(",") if x.strip()]


CHECKPOINTS = _ints("FT_PERF_CHECKPOINTS", "10,50,200,1000,4000")

FREQ_K = int(os.getenv("FT_PERF_FREQ_K", "4"))
FREQ_INTERVALS = {
    "small": _ints("FT_PERF_FREQ_INTERVALS_SMALL", "40,20,10,5,2,1"),
    "medium": _ints("FT_PERF_FREQ_INTERVALS_MEDIUM", "40,20,10,5,2,1"),
    "large": _ints("FT_PERF_FREQ_INTERVALS_LARGE", "40,10,2"),
}
BURST_INTERVAL = int(os.getenv("FT_PERF_BURST_INTERVAL", "20"))
BURST_KILLS = {
    "small": _ints("FT_PERF_BURST_KILLS_SMALL", "1,2,4,8,12,16,24"),
    "medium": _ints("FT_PERF_BURST_KILLS_MEDIUM", "1,2,4,8,12,16,24"),
    "large": _ints("FT_PERF_BURST_KILLS_LARGE", "2,8,16,24"),
}
DUAL_CHAOS_INTERVAL = int(os.getenv("FT_PERF_DUAL_CHAOS_INTERVAL", "40"))
CHAOS_CHECKPOINT = int(os.getenv("FT_PERF_CHAOS_CHECKPOINT", "1000"))
TOPOLOGY = os.getenv("FT_PERF_TOPOLOGY", "min2")
RAMP_TIERS = [t.strip() for t in os.getenv("FT_PERF_RAMP_TIERS", "small,medium,large").split(",")
              if t.strip()]


def _walk_to_cliff(phase, tier, checkpoint, steps, mk_args, label, do_build=False):
    """Shared cliff-walker: a no-chaos base, then each chaos step (reusing the hot cluster)
    until the run collapses twice in a row — the cliff. `mk_args` maps a step to the
    (interval, kills) pair for that step, so the same walk drives both sweeps."""
    measure(phase, tier, TOPOLOGY, checkpoint, False, 0, 0, do_build=do_build)
    streak = 0
    for step in steps:
        interval, kills = mk_args(step)
        row = measure(phase, tier, TOPOLOGY, checkpoint, True, interval, kills, new_cluster=False)
        if str(row.get("completed")) != "True":
            streak += 1
            if streak >= 2:
                log(f"  -> {tier}: collapsed twice in a row; {label} cliff found, stopping")
                break
        else:
            streak = 0


def _freq_ramp(tier, do_build=False):
    """Frequency sweep: fixed burst K, shrinking wave interval (X = interval seconds)."""
    _walk_to_cliff("F1", tier, CHAOS_CHECKPOINT, FREQ_INTERVALS[tier],
                   lambda iv: (iv, FREQ_K), "frequency", do_build=do_build)


def _burst_ramp(tier, do_build=False):
    """Burst sweep: fixed generous interval, growing wave size K (X = nodes per wave)."""
    _walk_to_cliff("F2", tier, CHAOS_CHECKPOINT, BURST_KILLS[tier],
                   lambda k: (BURST_INTERVAL, k), "burst", do_build=do_build)


def phase_F1():
    log(f"=== F1 frequency sweep (K={FREQ_K} fixed, vary interval) — tiers: {RAMP_TIERS} ===")
    for i, tier in enumerate(RAMP_TIERS):
        _freq_ramp(tier, do_build=(i == 0))


def phase_F2():
    log(f"=== F2 burst sweep (interval={BURST_INTERVAL}s fixed, vary K) — tiers: {RAMP_TIERS} ===")
    for tier in RAMP_TIERS:
        _burst_ramp(tier)


def phase_F3():
    log(f"=== F3 checkpoint dual-curve (medium, gentle chaos K={FREQ_K}@{DUAL_CHAOS_INTERVAL}s) ===")
    for ck in CHECKPOINTS:
        measure("F3", "medium", TOPOLOGY, ck, False, 0, 0)
        measure("F3", "medium", TOPOLOGY, ck, True, DUAL_CHAOS_INTERVAL, FREQ_K, new_cluster=False)


def maybe_plot():
    log("regenerating figures ...")
    r = run("uv run --with matplotlib --with numpy --with adjustText scripts/plot_ft_perf.py", capture=True)
    log(("figures OK" if r.returncode == 0 else f"plot failed: {r.stdout[-400:]}"))


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, newline="") as f:
            header = f.readline().strip()
        if header and header != ",".join(CSV_FIELDS):
            raise SystemExit(
                f"{RESULTS_CSV} has an outdated column schema; archive/remove it and re-run."
            )
    snapshot_files()
    phases = os.getenv("FT_PERF_PHASES", "F1,F2,F3").split(",")
    phases = [p.strip() for p in phases if p.strip()]
    log(f"benchmark start — topology: {TOPOLOGY} — phases: {phases}")
    try:
        if "F1" in phases:
            phase_F1()
        if "F2" in phases:
            phase_F2()
        if "F3" in phases:
            phase_F3()
    finally:
        teardown()
        restore_files()
        maybe_plot()
        log("benchmark done")


if __name__ == "__main__":
    sys.exit(main())
