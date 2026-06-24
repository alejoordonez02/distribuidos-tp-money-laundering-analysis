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
on the figures (the benchmark is re-run often) -- the shape carries the conclusion.

Figures carry no embedded title: the report sets the caption. Text is typeset with LaTeX so
the figures match the report's Computer Modern.

    uv run --with matplotlib --with numpy scripts/plot_ft_perf.py
"""

import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    from adjustText import adjust_text
except ImportError:
    adjust_text = None

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_CSV = os.path.join(ROOT, "tmp/ft_perf/results.csv")
OUT_DIR = os.path.join(ROOT, "doc/diagrams/v2")

TIER_LABEL = {"small": "Small", "medium": "Medium", "large": "Large"}
TIER_COLOR = {"small": "#2a9d8f", "medium": "#e76f51", "large": "#264653"}

FREQ = dict(phase="F1", xkey="chaos_interval",
            xlabel=r"Cada cuántos segundos cae una ráfaga (derecha $=$ más agresivo)",
            invert_x=True)
BURST = dict(phase="F2", xkey="kills_per_wave",
             xlabel="Nodos caídos por ráfaga (intervalo fijo)",
             invert_x=False)

try:
    plt.style.use("seaborn-v0_8-whitegrid")
except OSError:
    plt.style.use("ggplot")
plt.rcParams.update({
    "text.usetex": True,
    "font.family": "serif",
    "text.latex.preamble": r"\usepackage[utf8]{inputenc}\usepackage[T1]{fontenc}",
    "figure.dpi": 150,
    "font.size": 12,
    "axes.labelsize": 12,
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


def plot_sweep_all(rows, sweep, fname):
    """Every dataset on one relative-slowdown axis: how the stress axis degrades throughput.
    Each point is annotated with its slowdown vs base in %, colored by dataset, and a right
    axis reads the same scale directly as a percentage."""
    fig, ax = plt.subplots(figsize=(9.2, 4.8))
    series = []
    for tier in ("small", "medium", "large"):
        base = _base_secs(rows, sweep["phase"], tier)
        pts = _chaos_pts(rows, sweep, tier)
        if base is None or not pts:
            continue
        x = [_f(r[sweep["xkey"]]) for r in pts]
        y = [_f(r["total_s"]) / base for r in pts]
        ax.plot(x, y, "o-", color=TIER_COLOR[tier], label=TIER_LABEL[tier], zorder=3)
        series.append((tier, x, y))
    if not series:
        return

    ax.axhline(1.0, ls="--", color="#555", lw=1.5, label=r"sin degradación ($0\%$)")
    if sweep["invert_x"]:
        ax.invert_xaxis()
    ax.set_ylim(bottom=0.82)
    ax.set_ylim(top=ax.get_ylim()[1] * 1.12)  # headroom for the top label

    # label each point with its slowdown; adjustText repositions every label so none overlap
    # (with thin leader lines when it pulls one away), falling back to a per-column height-rank
    # stack if the library is absent.
    if adjust_text is not None:
        texts = [ax.text(xi, yi, rf"$+{(yi - 1.0) * 100:.0f}\%$", fontsize=8,
                         color=TIER_COLOR[tier], zorder=5)
                 for tier, x, y in series for xi, yi in zip(x, y)]
        adjust_text(texts, ax=ax,
                    arrowprops=dict(arrowstyle="-", color="0.55", lw=0.4))
    else:
        from collections import defaultdict
        column = defaultdict(list)
        for tier, x, y in series:
            for xi, yi in zip(x, y):
                column[xi].append((yi, tier))
        offsets = [8, 19, 30]
        for xi, pts in column.items():
            for rank, (yi, tier) in enumerate(sorted(pts)):
                ax.annotate(rf"$+{(yi - 1.0) * 100:.0f}\%$", (xi, yi),
                            textcoords="offset points", xytext=(0, offsets[min(rank, 2)]),
                            ha="center", fontsize=8, color=TIER_COLOR[tier], zorder=4)

    ax.set_xlabel(sweep["xlabel"])
    ax.set_ylabel(r"Ralentización relativa (tiempo $/$ base)")
    ax.legend(loc="upper left", frameon=True)
    ax2 = ax.twinx()
    lo, hi = ax.get_ylim()
    ax2.set_ylim((lo - 1.0) * 100, (hi - 1.0) * 100)
    ax2.set_ylabel(r"Sobrecosto vs base ($\%$)")
    ax2.grid(False)
    _save(fig, fname)


def plot_checkpoint(rows):
    """Single tier (the checkpoint cadence is dataset-agnostic): total time vs checkpoint_every,
    no-chaos vs chaos. The chaos curve stops where recovery stops converging -- the cliff."""
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

    fig, ax = plt.subplots(figsize=(8.4, 4.4))
    base_x = [c for c in xs if c in no_ch]
    chaos_x = [c for c in xs if c in ch]
    if base_x:
        ax.plot(base_x, [no_ch[c] for c in base_x], "o-", color="#2a9d8f", label="sin chaos")
    if chaos_x:
        ax.plot(chaos_x, [ch[c] for c in chaos_x], "s-", color="#e76f51", label="con chaos")
        # mark the cliff: the first checkpoint where chaos no longer completes
        last_ch = max(chaos_x)
        beyond = [c for c in base_x if c > last_ch]
        if beyond:
            ax.axvline(min(beyond), ls=":", color="#9b2226", lw=1.6, zorder=1)
            ax.annotate(r"el recovery deja de converger",
                        (min(beyond), ax.get_ylim()[1]), xytext=(-6, -8),
                        textcoords="offset points", ha="right", va="top",
                        fontsize=9, color="#9b2226")
    ax.set_xscale("log")
    ax.set_xlabel(r"\texttt{checkpoint\_every} (mensajes entre checkpoints, escala log)")
    ax.set_ylabel("Tiempo total (min)")
    ax.set_ylim(bottom=0)
    ax.legend(loc="best", frameon=True)
    _save(fig, "ft_perf_checkpoint.png")


def main():
    rows = load()
    if not rows:
        print("no results.csv yet -- nothing to plot")
        return
    plot_sweep_all(rows, FREQ, "ft_perf_frequency.png")
    plot_sweep_all(rows, BURST, "ft_perf_burst.png")
    plot_checkpoint(rows)
    print("done")


if __name__ == "__main__":
    main()
