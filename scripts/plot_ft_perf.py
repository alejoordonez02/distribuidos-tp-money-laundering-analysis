"""Render the Fault-Tolerance-vs-Performance figures from tmp/ft_perf/results.csv.

Invoked at the end of `make performance_vs_ft` (and standalone via `make perf_plots`).
Re-running overwrites the PNGs the report includes, so the figures always reflect the
latest measurements.

Three figures, one stress axis each, every dataset size on the same plot:
  - frequency sweep (how OFTEN a wave kills) -> relative slowdown, all tiers
  - burst sweep     (how MANY die per wave)  -> relative slowdown, all tiers
  - checkpoint curve (medium)                -> total time vs checkpoint_every, base vs chaos

The two sweeps use RELATIVE slowdown (time / no-chaos base) on purpose: the dataset sizes
span an order of magnitude, so absolute minutes don't share a single axis; normalising to
each dataset's own baseline makes the three directly comparable. No exact values are drawn
on the figures (the benchmark is re-run often) — the shape carries the conclusion.

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

TIER_LABEL = {"small": "Small", "medium": "Medium", "large": "Large"}
TIER_COLOR = {"small": "#2a9d8f", "medium": "#e76f51", "large": "#264653"}

FREQ = dict(phase="F1", xkey="chaos_interval",
            xlabel="Cada cuántos segundos se mata una ráfaga (más a la derecha = más agresivo)",
            invert_x=True)
BURST = dict(phase="F2", xkey="kills_per_wave",
             xlabel="Nodos derribados por ráfaga (intervalo fijo)",
             invert_x=False)

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
    "lines.linewidth": 2.4,
    "lines.markersize": 7,
})


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load():
    if not os.path.exists(RESULTS_CSV):
        return []
    with open(RESULTS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    # A run is plottable once it COMPLETED (it has a real wall-clock time). Whether its
    # output validated 5/5 is a correctness question, orthogonal to throughput.
    return [r for r in rows
            if str(r.get("completed")) == "True" and _f(r.get("total_s")) is not None]


def _save(fig, name):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"wrote {path}")


def _rows(rows, phase, tier):
    return [r for r in rows if r["tier"] == tier and r.get("phase") == phase]


def _base_secs(rows, phase, tier):
    base = [r for r in _rows(rows, phase, tier) if str(r.get("chaos_enabled")) != "True"]
    return min((_f(r["total_s"]) for r in base), default=None)


def _chaos_pts(rows, sweep, tier):
    pts = [r for r in _rows(rows, sweep["phase"], tier)
           if str(r.get("chaos_enabled")) == "True" and _f(r.get(sweep["xkey"]))]
    return sorted(pts, key=lambda r: _f(r[sweep["xkey"]]))


def plot_sweep_all(rows, sweep, title, fname):
    """Every dataset on one relative-slowdown axis: how the stress axis degrades throughput."""
    fig, ax = plt.subplots(figsize=(8.4, 4.2))
    drawn = False
    for tier in ("small", "medium", "large"):
        base = _base_secs(rows, sweep["phase"], tier)
        pts = _chaos_pts(rows, sweep, tier)
        if base is None or not pts:
            continue
        x = [_f(r[sweep["xkey"]]) for r in pts]
        y = [_f(r["total_s"]) / base for r in pts]
        ax.plot(x, y, "o-", color=TIER_COLOR[tier], label=TIER_LABEL[tier])
        drawn = True
    if not drawn:
        return
    ax.axhline(1.0, ls="--", color="#555", lw=1.5, label="sin degradación")
    if sweep["invert_x"]:
        ax.invert_xaxis()
    ax.set_ylim(bottom=0.9)
    ax.set_xlabel(sweep["xlabel"])
    ax.set_ylabel("Ralentización relativa (tiempo / base)")
    ax.set_title(title, pad=10)
    ax.legend(loc="upper left", frameon=True)
    _save(fig, fname)


def plot_checkpoint(rows):
    """Single tier (the checkpoint cadence is dataset-agnostic): total time vs checkpoint_every,
    no-chaos vs chaos. Shows the U — frequent checkpoints pay I/O, sparse ones pay reprocessing —
    and that failures tilt the balance toward more frequent checkpoints."""
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

    fig, ax = plt.subplots(figsize=(8.4, 4.2))
    if any(c in no_ch for c in xs):
        ax.plot([c for c in xs if c in no_ch], [no_ch[c] for c in xs if c in no_ch],
                "o-", color="#2a9d8f", label="sin chaos")
    if any(c in ch for c in xs):
        ax.plot([c for c in xs if c in ch], [ch[c] for c in xs if c in ch],
                "s-", color="#e76f51", label="con chaos")
    ax.set_xscale("log")
    ax.set_xlabel("checkpoint_every (mensajes entre checkpoints, escala log)")
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title("Checkpoint: hay un óptimo, y los fallos lo corren a checkpoints más frecuentes",
                 pad=10)
    ax.set_ylim(bottom=0)
    ax.legend(loc="best", frameon=True)
    _save(fig, "ft_perf_checkpoint.png")


def main():
    rows = load()
    if not rows:
        print("no results.csv yet — nothing to plot")
        return
    plot_sweep_all(rows, FREQ,
                   "Frecuencia: el costo se concentra solo en el régimen más agresivo",
                   "ft_perf_frequency.png")
    plot_sweep_all(rows, BURST,
                   "Ráfaga: la redundancia absorbe la simultaneidad, sin colapso",
                   "ft_perf_burst.png")
    plot_checkpoint(rows)
    print("done")


if __name__ == "__main__":
    main()
