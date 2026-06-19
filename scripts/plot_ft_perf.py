"""Render the Fault-Tolerance-vs-Performance figures from tmp/ft_perf/results.csv.

Invoked at the end of `make performance_vs_ft` (and standalone via `make perf_plots`).
Re-running overwrites the PNGs the report includes, so the figures always reflect the
latest measurements. Only charts whose data exists are drawn — a partial run still
produces the figures it can.

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

try:
    plt.style.use("seaborn-v0_8-whitegrid")
except OSError:
    plt.style.use("ggplot")
plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 12,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.labelsize": 12,
    "legend.fontsize": 11,
    "lines.linewidth": 2.2,
    "lines.markersize": 8,
})


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load():
    if not os.path.exists(RESULTS_CSV):
        print("no results.csv yet — nothing to plot")
        return []
    with open(RESULTS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    # keep only completed, validated runs with a real duration
    clean = []
    for r in rows:
        if str(r.get("completed")) != "True" or str(r.get("validated_5_5")) != "True":
            continue
        if _f(r.get("total_s")) is None:
            continue
        clean.append(r)
    return clean


def _save(fig, name):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"wrote {path}")


def baseline_for(rows, tier):
    base = [r for r in rows if r["tier"] == tier and str(r.get("chaos_enabled")) != "True"]
    if not base:
        return None
    return min(_f(r["total_s"]) for r in base) / 60.0  # minutes


def plot_interval(rows, tier):
    """Total time vs kill SPEED (waves/min), fixed kills/wave."""
    pts = [r for r in rows if r["tier"] == tier and str(r.get("chaos_enabled")) == "True"
           and _f(r.get("kills_per_wave")) == 1 and _f(r.get("chaos_interval"))]
    if not pts:
        return
    pts = sorted(pts, key=lambda r: 60.0 / _f(r["chaos_interval"]))
    x = [60.0 / _f(r["chaos_interval"]) for r in pts]
    y = [_f(r["total_s"]) / 60.0 for r in pts]
    base = baseline_for(rows, tier)

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    ax.plot(x, y, "o-", color=TIER_COLOR[tier], label="con chaos (1 nodo/oleada)")
    if base is not None:
        ax.axhline(base, ls="--", color="#555", lw=1.6, label=f"sin chaos (base = {base:.1f} min)")
        for xi, yi in zip(x, y):
            ax.annotate(f"+{(yi/base-1)*100:.0f}%", (xi, yi), textcoords="offset points",
                        xytext=(0, 9), ha="center", fontsize=9, color="#333")
    ax.set_xlabel("Velocidad de las oleadas (oleadas por minuto)")
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title(f"{TIER_LABEL[tier]}: degradación según la VELOCIDAD del chaos")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left", frameon=True)
    _save(fig, f"ft_perf_interval_{tier}.png")


def plot_kills(rows, tier):
    """Total time vs kills/wave (magnitude), fixed wave interval."""
    pts = [r for r in rows if r["tier"] == tier and str(r.get("chaos_enabled")) == "True"
           and _f(r.get("chaos_interval")) == 8 and _f(r.get("kills_per_wave"))]
    if not pts:
        return
    pts = sorted(pts, key=lambda r: _f(r["kills_per_wave"]))
    x = [_f(r["kills_per_wave"]) for r in pts]
    y = [_f(r["total_s"]) / 60.0 for r in pts]
    base = baseline_for(rows, tier)

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    ax.plot(x, y, "s-", color=TIER_COLOR[tier], label="con chaos (oleada cada 8 s)")
    if base is not None:
        ax.axhline(base, ls="--", color="#555", lw=1.6, label=f"sin chaos (base = {base:.1f} min)")
        for xi, yi in zip(x, y):
            ax.annotate(f"+{(yi/base-1)*100:.0f}%", (xi, yi), textcoords="offset points",
                        xytext=(0, 9), ha="center", fontsize=9, color="#333")
    ax.set_xlabel("Nodos derribados por oleada")
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title(f"{TIER_LABEL[tier]}: degradación según la MAGNITUD del chaos")
    ax.set_xticks(x)
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left", frameon=True)
    _save(fig, f"ft_perf_kills_{tier}.png")


def plot_checkpoint_dualcurve(rows):
    """The crossover: checkpoint_every helps without chaos, hurts under chaos."""
    tier = "medium"
    f2 = [r for r in rows if r.get("phase") == "F2" and r["tier"] == tier]
    if not f2:
        return
    base = sorted({_f(r["checkpoint_every"]) for r in f2})
    no_ch, ch = {}, {}
    for r in f2:
        ce = _f(r["checkpoint_every"])
        (ch if str(r.get("chaos_enabled")) == "True" else no_ch)[ce] = _f(r["total_s"]) / 60.0
    xs = [c for c in base if c in no_ch or c in ch]
    if not xs:
        return

    fig, ax = plt.subplots(figsize=(7.6, 4.8))
    if any(c in no_ch for c in xs):
        ax.plot([c for c in xs if c in no_ch], [no_ch[c] for c in xs if c in no_ch],
                "o-", color="#2a9d8f", label="sin chaos")
    if any(c in ch for c in xs):
        ax.plot([c for c in xs if c in ch], [ch[c] for c in xs if c in ch],
                "s-", color="#e76f51", label="con chaos (oleada cada 8 s, 1 nodo)")
    ax.set_xscale("log")
    ax.set_xlabel("checkpoint_every (mensajes entre checkpoints, escala log)")
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title("Medium: el checkpoint óptimo se INVIERTE bajo fallos")
    ax.set_ylim(bottom=0)
    ax.legend(loc="best", frameon=True)
    ax.annotate("más alto = menos I/O\n(mejor sin fallos)", (xs[-1], no_ch.get(xs[-1], 0)),
                textcoords="offset points", xytext=(-10, 20), ha="right", fontsize=9, color="#2a9d8f")
    _save(fig, "ft_perf_checkpoint_dualcurve.png")


def plot_overhead_summary(rows):
    """One bar chart across tiers: base vs a representative sustained-chaos run."""
    tiers = [t for t in ("small", "medium", "large") if any(r["tier"] == t for r in rows)]
    if not tiers:
        return
    bases, chaoses, labels = [], [], []
    for t in tiers:
        base = baseline_for(rows, t)
        rep = [r for r in rows if r["tier"] == t and str(r.get("chaos_enabled")) == "True"
               and _f(r.get("chaos_interval")) == 8 and _f(r.get("kills_per_wave")) == 1]
        if base is None or not rep:
            continue
        bases.append(base)
        chaoses.append(_f(rep[0]["total_s"]) / 60.0)
        labels.append(TIER_LABEL[t])
    if not labels:
        return

    import numpy as np
    x = np.arange(len(labels))
    w = 0.38
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    b1 = ax.bar(x - w / 2, bases, w, label="sin chaos", color="#8ecae6")
    b2 = ax.bar(x + w / 2, chaoses, w, label="con chaos (8 s, 1 nodo)", color="#fb8500")
    for rect, base, ch in zip(b2, bases, chaoses):
        ax.annotate(f"+{(ch/base-1)*100:.0f}%", (rect.get_x() + rect.get_width() / 2, ch),
                    textcoords="offset points", xytext=(0, 4), ha="center", fontsize=9)
    ax.set_xticks(x, labels)
    ax.set_ylabel("Tiempo total (min)")
    ax.set_title("Overhead de tolerancia a fallos bajo chaos sostenido")
    ax.legend(frameon=True)
    _save(fig, "ft_perf_overhead_summary.png")


def main():
    rows = load()
    if not rows:
        return
    for tier in ("small", "medium", "large"):
        plot_interval(rows, tier)
        plot_kills(rows, tier)
    plot_checkpoint_dualcurve(rows)
    plot_overhead_summary(rows)
    print("done")


if __name__ == "__main__":
    main()
