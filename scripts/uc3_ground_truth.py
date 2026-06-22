"""Ground-truth (sum, count, avg) of Amount Paid per Payment Format for UC3 period A
(USD, 2022/09/01..2022/09/06) over the small dataset — the reference the aggregate must
reproduce. Compare against the 'UC3DBG' lines logged by the aggregate under chaos.

    PYTHONPATH=src uv run scripts/uc3_ground_truth.py
"""

import pandas as pd

PATH = "datasets/LI-Small_Trans.csv"
CHUNK = 200_000

total: dict[str, float] = {}
count: dict[str, int] = {}
for chunk in pd.read_csv(PATH, chunksize=CHUNK):
    usd = chunk[chunk["Payment Currency"] == "US Dollar"]
    period_a = usd[(usd["Timestamp"] >= "2022/09/01") & (usd["Timestamp"] <= "2022/09/06")]
    if len(period_a) == 0:
        continue
    for fmt, grp in period_a.groupby("Payment Format"):
        k = str(fmt)
        total[k] = total.get(k, 0.0) + float(grp["Amount Paid"].sum())
        count[k] = count.get(k, 0) + len(grp)

print("=== UC3 ground truth (period A, USD, small) ===")
for fmt in sorted(total):
    print(f"fmt={fmt!r:<16} sum={total[fmt]:.4f}  count={count[fmt]}  avg={total[fmt]/count[fmt]:.6f}")
