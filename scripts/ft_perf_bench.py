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

Phases (select a subset with FT_PERF_PHASES=F0,F1,...):
  F0 sanity on perfect; F1 topology selection on medium; F2 checkpoint dual-curve on medium;
  F3 chaos sweeps on medium; F4 on small; F5 on large (reduced).

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

CSV_FIELDS = [
    "run_id", "ts", "phase", "tier", "topology", "checkpoint_every",
    "chaos_enabled", "chaos_interval", "kills_per_wave",
    "total_s", "validated_5_5", "completed",
    "n_kills", "n_revives", "mean_recovery_s", "error",
]

PROGRESS_INTERVAL = int(os.getenv("FT_PERF_PROGRESS", "5"))
STALL_GRACE = int(os.getenv("FT_PERF_STALL_GRACE", "180"))
WEDGE_GRACE = int(os.getenv("FT_PERF_WEDGE_GRACE", "240"))
DRAIN_TIMEOUT = int(os.getenv("FT_PERF_DRAIN", "180"))
CHAOS_START_DELAY = int(os.getenv("FT_PERF_CHAOS_START_DELAY", "3"))
CHAOS_EXCLUDE = "rabbitmq,supervisor,gateway,chaos"

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


def arm_chaos(enabled, interval, kills):
    env = (
        f"CHAOS_ENABLED={1 if enabled else 0} "
        f"CHAOS_INTERVAL={interval} "
        f"CHAOS_KILLS_MIN={kills} CHAOS_KILLS_MAX={kills} "
        f"CHAOS_START_DELAY={CHAOS_START_DELAY} "
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


def run_once(phase, tier, topology, checkpoint_every, chaos_on, interval, kills):
    """One measured client through the warm cluster. Returns a CSV row dict."""
    expected = expected_controllers()
    clear_responses()
    arm_chaos(chaos_on, interval, kills)

    start = time.time()
    recreate_client()
    completed = wait_for_client(tier, expected)
    total_s = int(time.time() - start)

    since = f"{total_s + 5}s"
    chaos_log = out(f"docker logs -t --since {since} chaos 2>&1", timeout=20) if chaos_on else ""
    sup_log = out(f"docker logs -t --since {since} supervisor 2>&1", timeout=20)
    metrics = parse_recovery_metrics(chaos_log, sup_log)

    arm_chaos(False, interval, kills)

    validated, detail, error = False, "", ""
    if not completed:
        error = "STALL/timeout: client did not finish within cap"
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

STALL_RETRY = os.getenv("FT_PERF_STALL_RETRY", "1") == "1"


def _error_row(run_id, phase, tier, topology, checkpoint, chaos_on, interval, kills, err):
    return {
        "run_id": run_id, "ts": datetime.now().isoformat(timespec="seconds"),
        "phase": phase, "tier": tier, "topology": topology,
        "checkpoint_every": checkpoint, "chaos_enabled": chaos_on,
        "chaos_interval": interval, "kills_per_wave": kills, "total_s": "",
        "validated_5_5": False, "completed": False, "n_kills": "", "n_revives": "",
        "mean_recovery_s": "", "error": err,
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
    """Run one measured client, persist a row, and return it. Never raises. A run that does
    not complete is retried once on a freshly rebuilt cluster (chaos stalls are flaky); a
    genuinely too-aggressive config stalls twice and is recorded as 'did not complete'."""
    global _cluster_dirty, _live_cluster
    run_id = f"{phase}-{tier}-{topology}-c{checkpoint}-{'chaos' if chaos_on else 'base'}-i{interval}-k{kills}"
    rows = load_rows()
    if already_done(rows, run_id):
        log(f"skip (already done): {run_id}")
        return next(r for r in rows if r.get("run_id") == run_id)

    log(f"RUN {run_id}")
    attempts = 2 if STALL_RETRY else 1
    row = None
    for attempt in range(1, attempts + 1):
        try:
            _ensure_cluster(tier, topology, checkpoint, do_build, force=(new_cluster or attempt == 2))
            row = run_once(phase, tier, topology, checkpoint, chaos_on, interval, kills)
        except Exception as e:
            row = _error_row(run_id, phase, tier, topology, checkpoint, chaos_on, interval, kills,
                             f"exception: {type(e).__name__}: {e}")
        if str(row.get("completed")) != "True":
            _cluster_dirty = True
            _live_cluster = None
            if attempt < attempts:
                log(f"  -> {row.get('total_s')}s did not complete; retrying once on a clean cluster ...")
                continue
        break

    append_row(row)
    if str(row.get("validated_5_5")) != "True":
        _cluster_dirty = True
        _live_cluster = None
    status = "OK 5/5" if str(row.get("validated_5_5")) == "True" else f"PROBLEM: {row.get('error')}"
    log(f"  -> {row.get('total_s')}s  {status}")
    return row


def _ints(env, default):
    return [int(x) for x in os.getenv(env, default).split(",") if x.strip()]


CHECKPOINTS = _ints("FT_PERF_CHECKPOINTS", "10,50,200,1000,4000")
SPEED_WAVES = _ints("FT_PERF_SPEED_WAVES", "3,6,12,24")
KILLS = _ints("FT_PERF_KILLS", "1,2,4,8")
MAG_WAVES = int(os.getenv("FT_PERF_MAG_WAVES", "6"))
CHAOS_CHECKPOINT = int(os.getenv("FT_PERF_CHAOS_CHECKPOINT", "200"))
TOPOLOGY = os.getenv("FT_PERF_TOPOLOGY", "min2")

BASE_SECS = {
    "small": int(os.getenv("FT_BASE_SMALL", "110")),
    "medium": int(os.getenv("FT_BASE_MEDIUM", "445")),
    "large": int(os.getenv("FT_BASE_LARGE", "1440")),
}


def _interval(tier, waves):
    return max(2, round(BASE_SECS[tier] / waves))


def _chaos_sweeps(phase, tier, checkpoint, speed_waves, kills_list, do_build=False):
    measure(phase, tier, TOPOLOGY, checkpoint, False, 0, 0, do_build=do_build)
    for w in speed_waves:
        measure(phase, tier, TOPOLOGY, checkpoint, True, _interval(tier, w), 1, new_cluster=False)
    mag_interval = _interval(tier, MAG_WAVES)
    for k in kills_list:
        measure(phase, tier, TOPOLOGY, checkpoint, True, mag_interval, k, new_cluster=False)


def phase_F1():
    log(f"=== F1 chaos sweeps (small, topo={TOPOLOGY}) ===")
    _chaos_sweeps("F1", "small", CHAOS_CHECKPOINT, SPEED_WAVES, KILLS, do_build=True)


def phase_F2():
    log(f"=== F2 chaos sweeps (medium, topo={TOPOLOGY}) ===")
    _chaos_sweeps("F2", "medium", CHAOS_CHECKPOINT, SPEED_WAVES, KILLS)


def phase_F3():
    log(f"=== F3 checkpoint dual-curve (medium, topo={TOPOLOGY}) ===")
    chaos_interval = _interval("medium", MAG_WAVES)
    for ck in CHECKPOINTS:
        measure("F3", "medium", TOPOLOGY, ck, False, 0, 0)
        measure("F3", "medium", TOPOLOGY, ck, True, chaos_interval, 1, new_cluster=False)


def phase_F4():
    log(f"=== F4 chaos sweeps (large reduced, topo={TOPOLOGY}) ===")
    red_speed = _ints("FT_PERF_LARGE_SPEED_WAVES", "6,12")
    red_kills = _ints("FT_PERF_LARGE_KILLS", "2,6")
    _chaos_sweeps("F4", "large", CHAOS_CHECKPOINT, red_speed, red_kills)


def maybe_plot():
    log("regenerating figures ...")
    r = run("uv run --with matplotlib --with numpy scripts/plot_ft_perf.py", capture=True)
    log(("figures OK" if r.returncode == 0 else f"plot failed: {r.stdout[-400:]}"))


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    snapshot_files()
    phases = os.getenv("FT_PERF_PHASES", "F1,F2,F3,F4").split(",")
    phases = [p.strip() for p in phases if p.strip()]
    log(f"benchmark start — topology: {TOPOLOGY} — phases: {phases}")
    try:
        if "F1" in phases:
            phase_F1()
        if "F2" in phases:
            phase_F2()
        if "F3" in phases:
            phase_F3()
        if "F4" in phases:
            phase_F4()
    finally:
        teardown()
        restore_files()
        maybe_plot()
        log("benchmark done")


if __name__ == "__main__":
    sys.exit(main())
