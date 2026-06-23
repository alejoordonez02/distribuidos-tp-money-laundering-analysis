"""Render the Fault-Tolerance-vs-Performance figures from tmp/ft_perf/results.csv.

Invoked at the end of `make performance_vs_ft` (and standalone via `make perf_plots`).
Re-running overwrites the PNGs the report includes, so the figures always reflect the
latest measurements. Only charts whose data exists are drawn — a partial run still
produces the figures it can.

Two sweeps, two failure modes (see ft_perf_bench.py), plotted with the SAME visual grammar:
stress grows left-to-right, the cliff sits on the right.
  F1 frequency sweep — X = wave interval in SECONDS (inverted: gentle 40s left, harsh 1s right).
  F2 burst sweep     — X = nodes killed per wave (1 left, 24 right).
  F3 checkpoint dual-curve.

Run with matplotlib injected ephemerally so the project deps stay untouched:
    uv run --with matplotlib --with numpy scripts/plot_ft_perf.py
"""

import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_CSV = os.path.join(ROOT, "tmp/ft_perf/results.csv")
OUT_DIR = os.path.join(ROOT, "doc/diagrams/v2")

TIER_LABEL = {"small": "Small", "medium": "Medium", "large": "Large", "perfect": "Perfect"}
TIER_COLOR = {"small": "#2a9d8f", "medium": "#e76f51", "large": "#264653", "perfect": "#999999"}
CLIFF_RED = "#c1121f"

# Each sweep: which phase holds it, the CSV column that varies, the axis label, whether the
# X axis is inverted (so stress always grows rightward), and on which side failures land.
FREQ = dict(phase="F1", xkey="chaos_interval",
            xlabel="Cada cuántos segundos se mata una ráfaga (más chico = más agresivo)",
            invert_x=True, cliff_is_max=True)
BURST = dict(phase="F2", xkey="kills_per_wave",
             xlabel="Nodos derribados por ráfaga (intervalo fijo de 20 s)",
             invert_x=False, cliff_is_max=False)

try:
    plt.style.use("seaborn-v0_8-whitegrid")
except OSError:
    plt.style.use("ggplot")
plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.labelsize": 11,
    "legend.fontsize": 10,
    "lines.linewidth": 2.2,
    "lines.markersize": 7,
})


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_raw():
    if not os.path.exists(RESULTS_CSV):
        return []
    with open(RESULTS_CSV, newline="") as f:
        return list(csv.DictReader(f))


def load():
    clean = []
    for r in load_raw():
        if str(r.get("completed")) != "True":
            continue
        if _f(r.get("total_s")) is None:
            continue
        clean.append(r)
    return clean


def _save(fig, name):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"wrote {path}")


def _rows(rows, phase, tier):
    return [r for r in rows if r["tier"] == tier and r.get("phase") == phase]


def _base_min(rows, phase, tier):
    base = [r for r in _rows(rows, phase, tier) if str(r.get("chaos_enabled")) != "True"]
    return min((_f(r["total_s"]) for r in base), default=None)


def _zoom(y, base_min):
    """Tight vertical framing (minutes): just enough headroom for the labels, no dead band."""
    vals = list(y) + ([base_min] if base_min is not None else [])
    lo, hi = min(vals), max(vals)
    pad = (hi - lo) * 0.12 + 0.01
    return lo - pad, hi + pad


def _cliff(raw, sweep, tier):
    """Boundary of collapse: the gentlest chaos step that never completed on any seed. A step
    that completed on some seed is not a cliff, which filters single-seed pathological stalls."""
    phase, xkey, is_max = sweep["phase"], sweep["xkey"], sweep["cliff_is_max"]
    done = {round(_f(r[xkey]), 3) for r in _rows(raw, phase, tier)
            if str(r.get("chaos_enabled")) == "True" and str(r.get("completed")) == "True"
            and _f(r.get(xkey))}
    failed = [_f(r[xkey]) for r in _rows(raw, phase, tier)
              if str(r.get("chaos_enabled")) == "True" and str(r.get("completed")) != "True"
              and _f(r.get(xkey)) and round(_f(r[xkey]), 3) not in done]
    if not failed:
        return None
    return max(failed) if is_max else min(failed)


def _chaos_pts(rows, sweep, tier):
    pts = [r for r in _rows(rows, sweep["phase"], tier)
           if str(r.get("chaos_enabled")) == "True" and _f(r.get(sweep["xkey"]))]
    return sorted(pts, key=lambda r: _f(r[sweep["xkey"]]))


def _fail_xs(raw, sweep, tier):
    """The chaos steps that collapsed (no real time to plot) — excluding any X that completed
    on another seed, so single-seed flukes don't get drawn as a cliff."""
    phase, xkey = sweep["phase"], sweep["xkey"]
    done = {round(_f(r[xkey]), 3) for r in _rows(raw, phase, tier)
            if str(r.get("chaos_enabled")) == "True" and str(r.get("completed")) == "True"
            and _f(r.get(xkey))}
    return sorted({_f(r[xkey]) for r in _rows(raw, phase, tier)
                   if str(r.get("chaos_enabled")) == "True" and str(r.get("completed")) != "True"
                   and _f(r.get(xkey)) and round(_f(r[xkey]), 3) not in done})


def plot_sweep(rows, raw, sweep, tier, title, fname):
    """Total time vs stress for one tier: degradation rising to the cliff. Flat aspect, tight
    framing, a thin cliff marker instead of a big empty red band."""
    base = _base_min(rows, sweep["phase"], tier)
    if base is None:
        return
    base_min = base / 60.0
    pts = _chaos_pts(rows, sweep, tier)
    if not pts:
        return
    x = [_f(r[sweep["xkey"]]) for r in pts]
    y = [_f(r["total_s"]) / 60.0 for r in pts]

    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.plot(x, y, "o-", color=TIER_COLOR[tier], label="con chaos", zorder=3)
    ax.axhline(base_min, ls="--", color="#555", lw=1.5,
               label=f"sin chaos (base = {base_min:.1f} min)")
    for xi, yi in zip(x, y):
        ax.annotate(f"+{(yi / base_min - 1) * 100:.0f}%", (xi, yi), textcoords="offset points",
                    xytext=(0, 8), ha="center", fontsize=8, color="#333")
    lo_y, hi_y = _zoom(y, base_min)
    xmin, xmax = min(x), max(x)
    fails = _fail_xs(raw, sweep, tier)
    cliff = _cliff(raw, sweep, tier)

    if fails:
        # A collapsed run records no real time, so the curve would just stop flat. Instead draw
        # a steep red rise to a capped "no termina" marker so the cliff reads as a blow-up.
        y_sym = max(y) * 1.5 + 0.05
        anchor = (x[0], y[0]) if sweep["cliff_is_max"] else (x[-1], y[-1])  # completed pt by the cliff
        edge = cliff if cliff is not None else (max(fails) if sweep["cliff_is_max"] else min(fails))
        ax.plot([anchor[0], edge], [anchor[1], y_sym], ls="--", color=CLIFF_RED, lw=1.8, zorder=2)
        ax.plot(fails, [y_sym] * len(fails), "^", color=CLIFF_RED, markersize=12, zorder=4,
                label="no termina (colapso)")
        ax.annotate("no termina", (edge, y_sym), textcoords="offset points", xytext=(0, 9),
                    ha="center", va="bottom", fontsize=9, color=CLIFF_RED, fontweight="bold")
        xmin, xmax = min([xmin] + fails), max([xmax] + fails)
        hi_y = max(hi_y, y_sym * 1.14)

    ax.set_ylim(lo_y, hi_y)
    if cliff is not None:
        ax.axvline(cliff, color=CLIFF_RED, ls=":", lw=1.6, zorder=1, alpha=0.7)
    span = (xmax - xmin) or 1.0
    ax.set_xlim(xmin - span * 0.06, xmax + span * 0.06)
    if sweep["invert_x"]:
        ax.invert_xaxis()

    ax.set_xlabel(sweep["xlabel"])
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title(title, pad=10)
    ax.legend(loc="upper left", frameon=True)
    _save(fig, fname)


def plot_sweep_all(rows, raw, sweep, title, fname):
    """Every tier on one relative-slowdown axis — do they collapse at the same point?"""
    fig, ax = plt.subplots(figsize=(8.2, 4.0))
    drawn = False
    for tier in ("small", "medium", "large"):
        base = _base_min(rows, sweep["phase"], tier)
        pts = _chaos_pts(rows, sweep, tier)
        if base is None or not pts:
            continue
        base_min = base / 60.0
        x = [_f(r[sweep["xkey"]]) for r in pts]
        y = [(_f(r["total_s"]) / 60.0) / base_min for r in pts]
        ax.plot(x, y, "o-", color=TIER_COLOR[tier], label=TIER_LABEL[tier])
        cliff = _cliff(raw, sweep, tier)
        if cliff is not None:
            ax.axvline(cliff, color=TIER_COLOR[tier], ls=":", lw=1.6)
        drawn = True
    if not drawn:
        return
    ax.axhline(1.0, ls="--", color="#555", lw=1.4, label="sin degradación (x1)")
    ax.set_xlabel(sweep["xlabel"])
    ax.set_ylabel("Ralentización relativa (tiempo / base)")
    ax.set_title(title, pad=10)
    if sweep["invert_x"]:
        ax.invert_xaxis()
    ax.legend(loc="upper left", frameon=True)
    _save(fig, fname)


def plot_checkpoint_dualcurve(rows):
    """The crossover: checkpoint_every helps without chaos, hurts under chaos."""
    tier = "medium"
    f3 = [r for r in rows if r.get("phase") == "F3" and r["tier"] == tier]
    if not f3:
        return
    cks = sorted({_f(r["checkpoint_every"]) for r in f3})
    no_ch, ch = {}, {}
    for r in f3:
        ce = _f(r["checkpoint_every"])
        (ch if str(r.get("chaos_enabled")) == "True" else no_ch)[ce] = _f(r["total_s"]) / 60.0
    xs = [c for c in cks if c in no_ch or c in ch]
    if not xs:
        return

    fig, ax = plt.subplots(figsize=(8.2, 4.0))
    if any(c in no_ch for c in xs):
        ax.plot([c for c in xs if c in no_ch], [no_ch[c] for c in xs if c in no_ch],
                "o-", color="#2a9d8f", label="sin chaos")
    if any(c in ch for c in xs):
        ax.plot([c for c in xs if c in ch], [ch[c] for c in xs if c in ch],
                "s-", color="#e76f51", label="con chaos (K=4)")
    ax.set_xscale("log")
    ax.set_xlabel("checkpoint_every (mensajes entre checkpoints, escala log)")
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title("Medium: el chaos penaliza más al checkpoint espaciado", pad=10)
    ax.set_ylim(bottom=0)
    ax.legend(loc="best", frameon=True)
    _save(fig, "ft_perf_checkpoint_dualcurve.png")


def plot_overhead_summary(rows):
    """One bar chart across tiers: base vs the mildest sustained-chaos run (frequency sweep)."""
    tiers = [t for t in ("small", "medium", "large")
             if _base_min(rows, FREQ["phase"], t) is not None]
    bases, chaoses, labels = [], [], []
    for t in tiers:
        base = _base_min(rows, FREQ["phase"], t)
        ch = _chaos_pts(rows, FREQ, t)
        if base is None or not ch:
            continue
        rep = max(ch, key=lambda r: _f(r["chaos_interval"]))  # gentlest = largest interval
        bases.append(base / 60.0)
        chaoses.append(_f(rep["total_s"]) / 60.0)
        labels.append(TIER_LABEL[t])
    if not labels:
        return

    import numpy as np
    x = np.arange(len(labels))
    w = 0.38
    fig, ax = plt.subplots(figsize=(7.6, 4.0))
    ax.bar(x - w / 2, bases, w, label="sin chaos", color="#8ecae6")
    b2 = ax.bar(x + w / 2, chaoses, w, label="con chaos suave (K=4)", color="#fb8500")
    for rect, base, ch in zip(b2, bases, chaoses):
        ax.annotate(f"+{(ch / base - 1) * 100:.0f}%", (rect.get_x() + rect.get_width() / 2, ch),
                    textcoords="offset points", xytext=(0, 4), ha="center", fontsize=9)
    ax.set_xticks(x, labels)
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title("Overhead de tolerancia a fallos bajo chaos suave", pad=10)
    ax.legend(frameon=True)
    _save(fig, "ft_perf_overhead_summary.png")


def main():
    rows = load()
    raw = load_raw()
    if not rows:
        print("no results.csv yet — nothing to plot")
        return
    for tier in ("small", "medium", "large"):
        plot_sweep(rows, raw, FREQ, tier,
                   f"{TIER_LABEL[tier]} · frecuencia: degradación y punto de quiebre",
                   f"ft_perf_freq_{tier}.png")
        plot_sweep(rows, raw, BURST, tier,
                   f"{TIER_LABEL[tier]} · ráfaga: degradación sostenida, sin colapso",
                   f"ft_perf_burst_{tier}.png")
    plot_sweep_all(rows, raw, FREQ,
                   "Frecuencia: el quiebre depende de la exposición, no solo del kill-rate",
                   "ft_perf_freq_all.png")
    plot_sweep_all(rows, raw, BURST,
                   "Ráfaga: cuántos nodos de golpe tolera cada tier",
                   "ft_perf_burst_all.png")
    plot_checkpoint_dualcurve(rows)
    plot_overhead_summary(rows)
    print("done")


if __name__ == "__main__":
    main()
