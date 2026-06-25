# Adapted from https://www.kaggle.com/code/pablodroca/money-laundering-analysis to emit per-client input + expected output.

import gc
import os
import sqlite3
import tempfile
from collections import defaultdict
from datetime import date, datetime

import numpy as np
import pandas as pd
from cfg import (
    ACCOUNTS_PATH,
    ACCOUNTS_SAMPLE_SIZE,
    CLIENT_DATASETS_PATH,
    CLIENT_EXPECTED_RESPONSES_PATH,
    NCLIENTS,
    TRANSACTIONS_PATH,
    TRANSACTIONS_SAMPLE_FRAC,
)

from pandas.core.generic import DtypeArg # type: ignore

from common.conversion import FrankfurterConversionAPI

RANDOM_SEED = 2026

_CHUNK_SIZE = 50_000

# Optimized dtypes; Amount Paid stays float64 to keep precision in expected-output comparisons.
_TRANS_DTYPE: DtypeArg = {
    "From Bank": "int32",
    "To Bank": "int32",
    "Account": "category",
    "Account.1": "category",
    "Payment Currency": "category",
    "Payment Format": "category",
    "Is Laundering": "int8",
}

_ACCOUNTS_DTYPE: DtypeArg = {
    "Bank ID": "int32",
}

# Binance BTCUSDT daily closing prices (same as UC5USDConverterFn)
_BITCOIN_RATES_USD: dict[date, float] = {
    date(2022, 9, 1): 20131.46,
    date(2022, 9, 2): 19951.86,
    date(2022, 9, 3): 19831.90,
    date(2022, 9, 4): 20000.30,
    date(2022, 9, 5): 19796.84,
}

_conversion_api = FrankfurterConversionAPI()
_rate_cache: dict[date, dict[str, float]] = {}


def _log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _get_rates(date_slash: str) -> dict[str, float]:
    day = date.fromisoformat(date_slash.replace("/", "-"))
    if day not in _rate_cache:
        rates = _conversion_api.get_rates(day)
        if day in _BITCOIN_RATES_USD:
            rates["Bitcoin"] = _BITCOIN_RATES_USD[day]
        _rate_cache[day] = rates
    return _rate_cache[day]


def main():
    """
    Generate the input and expected output for each client.

    Mode selection driven by TRANSACTIONS_SAMPLE_FRAC (= 1 / NCLIENTS):
      - Streaming (frac == 1.0): each client gets a symlink to the full dataset.
        Used when NCLIENTS=1. UC4 uses an on-disk SQLite accumulator so it never OOMs.
      - Sampled (frac < 1.0): each client gets a fresh random sample of
        floor(n_total * TRANSACTIONS_SAMPLE_FRAC) rows. Used when NCLIENTS > 1.
    """
    _log("=== gen_input_output START ===")
    _log(
        f"NCLIENTS={NCLIENTS}, TRANSACTIONS_SAMPLE_FRAC={TRANSACTIONS_SAMPLE_FRAC}, "
        f"ACCOUNTS_SAMPLE_SIZE={ACCOUNTS_SAMPLE_SIZE}"
    )
    rng = np.random.default_rng(seed=RANDOM_SEED)

    _log(f"Loading accounts from {ACCOUNTS_PATH} ...")
    if ACCOUNTS_SAMPLE_SIZE is not None:
        accounts_df = gen_sampled_dataframe(
            ACCOUNTS_SAMPLE_SIZE,
            ACCOUNTS_PATH,
            CLIENT_DATASETS_PATH + "accounts.csv",
            rng,
            dtype=_ACCOUNTS_DTYPE,
        )
    else:
        accounts_df = pd.read_csv(ACCOUNTS_PATH, dtype=_ACCOUNTS_DTYPE)
    _log(f"Accounts loaded: {len(accounts_df)} rows")

    use_streaming = TRANSACTIONS_SAMPLE_FRAC >= 1.0

    if use_streaming:
        _log("STREAMING MODE — full dataset per client (NCLIENTS=1, no size cap)")
        for n in range(NCLIENTS):
            _log(f"--- Client {n + 1}/{NCLIENTS} ---")
            # Symlink to the full dataset (no 15GB copy); the client normalizes bank ids in its parser, so raw matches the oracle.
            dst = CLIENT_DATASETS_PATH + f"transactions_{n}.csv"
            if os.path.lexists(dst):
                os.remove(dst)
            _link_or_warn(TRANSACTIONS_PATH, dst)
            gen_results_streaming(
                TRANSACTIONS_PATH,
                accounts_df,
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc1_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc2_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc3_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc4_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc5_{n}.csv",
            )
            gc.collect()
            _log(f"Client {n + 1} done — expected responses written to {CLIENT_EXPECTED_RESPONSES_PATH}")
    else:
        _log(f"Counting rows in {TRANSACTIONS_PATH} ...")
        with open(TRANSACTIONS_PATH, "rb") as f:
            n_total = sum(1 for _ in f) - 1
        per_client_size = max(1, int(n_total * TRANSACTIONS_SAMPLE_FRAC))
        _log(
            f"SAMPLED MODE — {TRANSACTIONS_SAMPLE_FRAC:.4f} of dataset per client "
            f"({per_client_size}/{n_total} rows)"
        )

        for n in range(NCLIENTS):
            _log(f"--- Client {n + 1}/{NCLIENTS} ---")
            _log(f"Sampling transactions from {TRANSACTIONS_PATH} ...")
            trans_df = gen_sampled_dataframe(
                per_client_size,
                TRANSACTIONS_PATH,
                CLIENT_DATASETS_PATH + f"transactions_{n}.csv",
                rng,
                dtype=_TRANS_DTYPE,
                n_total=n_total,
            )
            _log(
                f"Transactions sampled: {len(trans_df)} rows → wrote datasets/transactions_{n}.csv"
            )
            gen_results(
                trans_df,
                accounts_df,
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc1_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc2_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc3_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc4_{n}.csv",
                CLIENT_EXPECTED_RESPONSES_PATH + f"uc5_{n}.csv",
            )
            del trans_df
            gc.collect()
            _log(
                f"Client {n + 1} done — expected responses written to {CLIENT_EXPECTED_RESPONSES_PATH}"
            )

    _log("=== gen_input_output DONE — all files ready ===")


# ── Streaming mode ────────────────────────────────────────────────────────────


def _link_or_warn(src: str, dst: str) -> None:
    """Create a relative symlink at dst → src. Relative so it resolves correctly inside Docker."""
    abs_src = os.path.abspath(src)
    rel_src = os.path.relpath(abs_src, start=os.path.dirname(os.path.abspath(dst)))
    if os.path.islink(dst):
        if os.path.realpath(dst) == abs_src:
            _log(f"  Symlink already correct: {dst} → {rel_src}")
            return
        os.unlink(dst)
    if os.path.exists(dst):
        _log(f"  WARNING: {dst} is a real file — skipping symlink creation. Delete it manually if you want it re-linked.")
        return
    os.symlink(rel_src, dst)
    _log(f"  Symlinked {dst} → {rel_src}")


class _CsvSink:
    """Append rows to a CSV, reproducing exactly what
    ``pd.concat(chunks, ignore_index=True).to_csv(path)`` would have written:
    a continuous integer index column plus a single header line.

    Writing chunk-by-chunk keeps RAM bounded — the full result set never lives
    in memory at once (the old streaming helpers accumulated every matching row
    in a list and concatenated at the end, which OOMs on the Large dataset).
    """

    def __init__(self, path: str, columns: list[str]):
        self._path = path
        self._columns = columns
        self._offset = 0
        self._first = True

    def write(self, df) -> None:
        if len(df) == 0:
            return
        df = df.copy()
        df.index = range(self._offset, self._offset + len(df))
        self._offset += len(df)
        df.to_csv(self._path, mode="w" if self._first else "a", header=self._first)
        self._first = False

    def close(self) -> int:
        if self._first:
            # No rows matched — emit a header-only file, like an empty DataFrame's to_csv.
            pd.DataFrame(columns=self._columns).to_csv(self._path)
        return self._offset


def gen_results_streaming(
    path: str,
    accounts_df,
    uc1_results_path: str,
    uc2_results_path: str,
    uc3_results_path: str,
    uc4_results_path: str,
    uc5_results_path: str,
):
    """
    Streaming version of gen_results. Makes multiple single-pass reads over the
    source file so the full dataset never has to be in RAM at once.

    Pass budget (each pass reads the full file once):
      UC1 — 1 pass
      UC2 — 1 pass
      UC3 — 2 passes (period-A averages, then period-B filter)
      UC4 — 1 pass  (edge collection) + in-memory graph
      UC5 — 1 pass
    """
    _log("  UC1: pass 1/1 ...")
    n_uc1 = _uc1_streaming(path, uc1_results_path)
    _log(f"  UC1: {n_uc1} rows → {uc1_results_path}")
    gc.collect()

    _log("  UC2: pass 1/1 ...")
    uc2_results = _uc2_streaming(path, accounts_df)
    uc2_results.to_csv(uc2_results_path)
    _log(f"  UC2: {len(uc2_results)} rows → {uc2_results_path}")
    del uc2_results
    gc.collect()

    _log("  UC3: pass 1/2 (period-A averages) ...")
    avg_per_fmt = _uc3_pass1(path)
    _log(f"  UC3: pass 2/2 (period-B filter) ...")
    n_uc3 = _uc3_pass2(path, avg_per_fmt, uc3_results_path)
    _log(f"  UC3: {n_uc3} rows → {uc3_results_path}")
    gc.collect()

    _log("  UC4: pass 1/1 (edge collection + graph) ...")
    uc4_results = _uc4_streaming(path)
    uc4_results.to_csv(uc4_results_path)
    _log(f"  UC4: {len(uc4_results)} rows → {uc4_results_path}")
    del uc4_results
    gc.collect()

    _log("  UC5: pass 1/1 ...")
    uc5_result = _uc5_streaming(path)
    with open(uc5_results_path, "w") as f:
        f.write(str(uc5_result))
    _log(f"  UC5: result={uc5_result} → {uc5_results_path}")


def _uc1_streaming(path: str, out_path: str) -> int:
    """USD transactions with Amount Paid < 50, written straight to disk so the
    full result set never accumulates in RAM."""
    cols = ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"]
    sink = _CsvSink(out_path, cols)
    for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=_TRANS_DTYPE):
        mask = (chunk["Payment Currency"] == "US Dollar") & (chunk["Amount Paid"] < 50)
        sink.write(chunk.loc[mask, cols])
    return sink.close()


def _uc2_streaming(path: str, accounts_df) -> pd.DataFrame:
    """Max USD transaction per source bank, joined with bank names."""
    # best[bank_id] = {"From Bank": int, "Account": str, "Amount Paid": float}
    best: dict[int, dict] = {}

    for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=_TRANS_DTYPE):
        usd = chunk[chunk["Payment Currency"] == "US Dollar"]
        if len(usd) == 0:
            continue
        # One candidate row per bank in this chunk
        idx = usd.groupby("From Bank")["Amount Paid"].idxmax()
        candidates = usd.loc[idx.values, ["From Bank", "Account", "Amount Paid"]]
        for row in candidates.itertuples(index=False):
            bank = int(row[0])
            amt = float(row[2])
            if bank not in best or amt > best[bank]["Amount Paid"]:
                best[bank] = {"From Bank": bank, "Account": str(row[1]), "Amount Paid": amt}

    if not best:
        return pd.DataFrame(columns=["From Bank", "Account", "Bank Name", "Amount Paid"])

    result_df = pd.DataFrame(best.values())
    bank_names = accounts_df.drop_duplicates("Bank ID")[["Bank ID", "Bank Name"]]
    merged = result_df.merge(bank_names, left_on="From Bank", right_on="Bank ID")
    return merged[["From Bank", "Account", "Bank Name", "Amount Paid"]]


def _uc3_pass1(path: str) -> dict[str, float]:
    """
    Pass 1: compute avg Amount Paid per Payment Format for period A
    (USD, Timestamp >= 2022/09/01 AND <= 2022/09/06 — effectively Sep 1-5 due to
    string comparison with "YYYY/MM/DD HH:MM" format).
    """
    total: dict[str, float] = {}
    count: dict[str, int] = {}
    for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=_TRANS_DTYPE):
        usd = chunk[chunk["Payment Currency"] == "US Dollar"]
        period_a = usd[
            (usd["Timestamp"] >= "2022/09/01") & (usd["Timestamp"] <= "2022/09/06")
        ]
        if len(period_a) == 0:
            continue
        for fmt, grp in period_a.groupby("Payment Format"):
            k = str(fmt)
            total[k] = total.get(k, 0.0) + float(grp["Amount Paid"].sum())
            count[k] = count.get(k, 0) + len(grp)
    return {fmt: total[fmt] / count[fmt] for fmt in total}


def _uc3_pass2(path: str, avg_per_fmt: dict[str, float], out_path: str) -> int:
    """
    Pass 2: filter period B (USD, Sep 6-15) rows where Amount Paid < avg/100.
    Written straight to disk so the matching rows never accumulate in RAM.
    """
    cols = ["From Bank", "Account", "Payment Format", "Amount Paid"]
    sink = _CsvSink(out_path, cols)
    for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=_TRANS_DTYPE):
        usd = chunk[chunk["Payment Currency"] == "US Dollar"]
        period_b = usd[
            (usd["Timestamp"] >= "2022/09/06") & (usd["Timestamp"] < "2022/09/16")
        ]
        if len(period_b) == 0:
            continue
        period_b = period_b.copy()
        period_b["_avg"] = period_b["Payment Format"].map(avg_per_fmt).astype("float64")
        matching = period_b[
            period_b["_avg"].notna() & (period_b["Amount Paid"] < period_b["_avg"] * 0.01)
        ]
        sink.write(matching[cols])
    return sink.close()


def _uc4_pairs_to_accounts(
    succs_of: dict[int, set[int]],
    preds_of: dict[int, set[int]],
    id_to_node: list[tuple],
) -> pd.DataFrame:
    """
    Count (a,c) pairs using an on-disk SQLite accumulator so the pair_count never
    lives in RAM. Hub nodes with thousands of predecessors/successors would otherwise
    generate O(N²) dict entries and OOM the machine.
    """
    db_fd, db_path = tempfile.mkstemp(suffix=".db", prefix="uc4_pairs_")
    os.close(db_fd)
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA cache_size=-65536")  # 64 MB page cache
        conn.execute(
            "CREATE TABLE pc (a INT NOT NULL, c INT NOT NULL, cnt INT NOT NULL, "
            "PRIMARY KEY (a, c)) WITHOUT ROWID"
        )
        conn.commit()

        _PAIR_BATCH = 100_000
        batch: list[tuple[int, int]] = []

        def _flush() -> None:
            if batch:
                conn.executemany(
                    "INSERT INTO pc(a,c,cnt) VALUES(?,?,1) "
                    "ON CONFLICT(a,c) DO UPDATE SET cnt=cnt+1",
                    batch,
                )
                conn.commit()
                batch.clear()

        for b, cs in succs_of.items():
            if b not in preds_of:
                continue
            for a in preds_of[b]:
                for c in cs:
                    if a != c:
                        batch.append((a, c))
                if len(batch) >= _PAIR_BATCH:
                    _flush()

        _flush()

        qualifying = conn.execute("SELECT a, c FROM pc WHERE cnt >= 5").fetchall()
        conn.close()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

    accounts: set[tuple] = set()
    for a, c in qualifying:
        accounts.add(id_to_node[a])
        accounts.add(id_to_node[c])
    return pd.DataFrame([(bank, acc) for bank, acc in accounts], columns=["Bank", "Account"])


def _uc4_streaming(path: str) -> pd.DataFrame:
    """
    Scatter-gather graph: collect Sep 1-5 USD edges in a single pass,
    then find (A, C) pairs with >= 5 distinct intermediaries B (A→B and B→C).

    Memory design:
      - succs_of / preds_of live in RAM (O(unique_edges) — manageable).
      - pair_count is stored in a temp SQLite file so hub nodes with thousands of
        predecessors/successors don't cause an O(N²) RAM explosion.
      - Both adjacency maps are built in one pass over the deduped edge list.
    """
    edge_chunks = []
    for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=_TRANS_DTYPE):
        usd = chunk[chunk["Payment Currency"] == "US Dollar"]
        period = usd[
            (usd["Timestamp"] >= "2022/09/01") & (usd["Timestamp"] <= "2022/09/06")
        ]
        if len(period) == 0:
            continue
        edge_chunks.append(
            period[["From Bank", "Account", "To Bank", "Account.1"]].copy()
        )

    if not edge_chunks:
        return pd.DataFrame(columns=["Bank", "Account"])

    edges_df = pd.concat(edge_chunks, ignore_index=True).drop_duplicates()
    del edge_chunks
    gc.collect()

    node_to_id: dict[tuple, int] = {}
    id_to_node: list[tuple] = []

    def node_id(bank, account) -> int:
        key = (bank, account)
        if key not in node_to_id:
            node_to_id[key] = len(id_to_node)
            id_to_node.append(key)
        return node_to_id[key]

    edges: list[tuple[int, int]] = [
        (node_id(int(row[0]), str(row[1])), node_id(int(row[2]), str(row[3])))
        for row in edges_df.itertuples(index=False)
    ]
    del edges_df
    gc.collect()

    # Build forward (succs_of) AND backward (preds_of) adjacency in one pass
    succs_of: dict[int, set[int]] = defaultdict(set)
    preds_of: dict[int, set[int]] = defaultdict(set)
    for a, b in edges:
        succs_of[a].add(b)
        preds_of[b].add(a)
    del edges
    gc.collect()

    result = _uc4_pairs_to_accounts(succs_of, preds_of, id_to_node)
    del succs_of, preds_of
    gc.collect()
    return result


def _uc5_streaming(path: str) -> int:
    """
    Count Wire/ACH transactions in Sep 1-5 whose amount converted to USD is < 1.
    Frankfurter API is called at most once per day (5 calls total, then cached).
    """
    count = 0
    for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=_TRANS_DTYPE):
        period = chunk[
            (chunk["Timestamp"] >= "2022/09/01") & (chunk["Timestamp"] < "2022/09/06")
        ]
        wire_ach = period[
            (period["Payment Format"] == "Wire") | (period["Payment Format"] == "ACH")
        ].copy()
        if len(wire_ach) == 0:
            continue
        dates = wire_ach["Timestamp"].str[:10].tolist()
        currencies = wire_ach["Payment Currency"].tolist()
        amounts = wire_ach["Amount Paid"].tolist()
        wire_ach["USD Amount"] = [
            amt * _get_rates(d).get(c, 1.0)
            for amt, d, c in zip(amounts, dates, currencies)
        ]
        count += wire_ach[wire_ach["USD Amount"] < 1.0].shape[0]
    return count


# ── Sampled mode (unchanged) ──────────────────────────────────────────────────


def _chunked_sample(
    path: str,
    n: int,
    rng: np.random.Generator,
    dtype: DtypeArg | None = None,
    n_total: int | None = None,
) -> pd.DataFrame:
    """Read CSV in chunks, returning n randomly sampled rows without loading the full file."""
    if n_total is None:
        with open(path, "rb") as f:
            n_total = sum(1 for _ in f) - 1  # exclude header

    if n >= n_total:
        # Read in chunks even for the full file — avoids a single large pd.read_csv call
        chunks = []
        for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=dtype):
            chunks.append(chunk)
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    chosen = set(int(i) for i in rng.choice(n_total, size=n, replace=False))
    chunks = []
    global_row = 0

    for chunk in pd.read_csv(path, chunksize=_CHUNK_SIZE, dtype=dtype):
        chunk_len = len(chunk)
        local_indices = [
            i - global_row
            for i in range(global_row, global_row + chunk_len)
            if i in chosen
        ]
        if local_indices:
            chunks.append(chunk.iloc[local_indices])
        global_row += chunk_len

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def gen_sampled_dataframe(
    sample_size: int | None,
    dataframe_path: str,
    sampled_path: str,
    rng: np.random.Generator,
    dtype: DtypeArg | None = None,
    n_total: int | None = None,
):
    """
    Samples a dataset, writes it to its corresponding path and returns it.
    When sample_size is None, loads the full file with dtype optimization.
    When sample_size is set, uses chunked reading to avoid loading the full file.
    Pass n_total to skip the internal line count in _chunked_sample.
    """
    if sample_size is None:
        sampled_df = pd.read_csv(dataframe_path, dtype=dtype)
    else:
        sampled_df = _chunked_sample(dataframe_path, sample_size, rng, dtype=dtype, n_total=n_total)

    sampled_df.to_csv(sampled_path, index=False)
    return sampled_df


def gen_results(
    trans_df,
    accounts_df,
    uc1_results_path: str,
    uc2_results_path: str,
    uc3_results_path: str,
    uc4_results_path: str,
    uc5_results_path: str,
):
    """
    Generates the results for all use cases and writes them to the specified files.
    """
    _log("  UC1: generating results ...")
    uc1_results = gen_uc1_results(trans_df)
    uc1_results.to_csv(uc1_results_path)
    _log(f"  UC1: {len(uc1_results)} rows → {uc1_results_path}")

    _log("  UC2: generating results ...")
    uc2_results = gen_uc2_results(trans_df, accounts_df)
    uc2_results.to_csv(uc2_results_path)
    _log(f"  UC2: {len(uc2_results)} rows → {uc2_results_path}")

    _log("  UC3: generating results ...")
    uc3_results = gen_uc3_results(trans_df)
    uc3_results.to_csv(uc3_results_path)
    _log(f"  UC3: {len(uc3_results)} rows → {uc3_results_path}")

    _log("  UC4: generating results (graph computation) ...")
    uc4_results = gen_uc4_results(trans_df)
    uc4_results.to_csv(uc4_results_path)
    _log(f"  UC4: {len(uc4_results)} rows → {uc4_results_path}")

    _log("  UC5: generating results (Frankfurter API calls) ...")
    uc5_results = gen_uc5_results(trans_df)
    with open(uc5_results_path, "w") as f:
        f.write(str(uc5_results))
    _log(f"  UC5: result={uc5_results} → {uc5_results_path}")


def gen_uc1_results(trans_df):
    """
    Returns a `DataFrame` with the results.
    """
    trans_usd_df = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    low_profile_transactions = trans_usd_df[trans_usd_df["Amount Paid"] < 50]
    return low_profile_transactions[
        ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"]
    ]


def gen_uc2_results(trans_df, accounts_df):
    """
    Returns a `DataFrame` with the results.
    """
    trans_usd_df = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    max_amount_trans_usd_idx = trans_usd_df.groupby(["From Bank"])[
        "Amount Paid"
    ].idxmax()
    max_amount_trans_usd = trans_usd_df.loc[max_amount_trans_usd_idx]
    # Each bank_id maps to one bank_name; dedup before merging to avoid a many-to-many explosion.
    bank_names = accounts_df.drop_duplicates("Bank ID")[["Bank ID", "Bank Name"]]
    max_amount_bank = max_amount_trans_usd.merge(
        bank_names, left_on="From Bank", right_on="Bank ID"
    )
    return max_amount_bank[["From Bank", "Account", "Bank Name", "Amount Paid"]]


def gen_uc3_results(trans_df):
    """
    Source account, payment format, and amount of transactions in period [2022-09-06,
    2022-11-06] with amount lower than AVG/100 of period [2022-09-01, 2022-09-05] for
    the same type of transaction.

    Returns a `DataFrame` with the results.
    """
    trans_usd_df = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    trans_usd_sept_1st_df = trans_usd_df[
        (trans_usd_df["Timestamp"] >= "2022/09/01")
        & (trans_usd_df["Timestamp"] <= "2022/09/06")
    ]
    avg_amounts_per_type = (
        trans_usd_sept_1st_df.groupby(["Payment Format"])["Amount Paid"]
        .mean()
        .reset_index()
    )
    trans_usd_sept_2nd_df = trans_usd_df[
        (trans_usd_df["Timestamp"] >= "2022/09/06")
        & (trans_usd_df["Timestamp"] < "2022/09/16")
    ]
    trans_usd_sept_2nd_with_avg_df = trans_usd_sept_2nd_df.merge(
        avg_amounts_per_type, left_on=["Payment Format"], right_on=["Payment Format"]
    ).rename(columns={"Amount Paid_x": "Amount Paid", "Amount Paid_y": "AVG"})
    lower_trans = trans_usd_sept_2nd_with_avg_df[
        trans_usd_sept_2nd_with_avg_df["Amount Paid"]
        < trans_usd_sept_2nd_with_avg_df["AVG"] * 0.01
    ]
    return lower_trans[["From Bank", "Account", "Payment Format", "Amount Paid"]]


def gen_uc4_results(trans_df):
    """
    Accounts that match the scatter-gather pattern with a single separation account,
    where the source account transferred to at least 5 distinct accounts in USD
    within the period [2022-09-01, 2022-09-05].

    For each pair (A, C), counts the distinct intermediary accounts B such that A→B
    and B→C. Keeps pairs with at least 5 distinct B's.

    Returns a `DataFrame` with columns [Bank, Account].
    """
    trans_usd_df = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    trans_usd_sept_1st_df = trans_usd_df[
        (trans_usd_df["Timestamp"] >= "2022/09/01")
        & (trans_usd_df["Timestamp"] <= "2022/09/06")
    ]

    edges_df = trans_usd_sept_1st_df[
        ["From Bank", "Account", "To Bank", "Account.1"]
    ].drop_duplicates()

    # Encode each (bank, account) node as an integer to minimize memory usage
    node_to_id: dict[tuple, int] = {}
    id_to_node: list[tuple] = []

    def node_id(bank, account) -> int:
        key = (bank, account)
        if key not in node_to_id:
            node_to_id[key] = len(id_to_node)
            id_to_node.append(key)
        return node_to_id[key]

    edges: list[tuple[int, int]] = [
        (node_id(row[0], row[1]), node_id(row[2], row[3]))
        for row in edges_df.itertuples(index=False)
    ]
    del edges_df

    succs_of: dict[int, set[int]] = defaultdict(set)
    preds_of: dict[int, set[int]] = defaultdict(set)
    for a, b in edges:
        succs_of[a].add(b)
        preds_of[b].add(a)
    del edges

    result = _uc4_pairs_to_accounts(succs_of, preds_of, id_to_node)
    del succs_of, preds_of
    return result


def gen_uc5_results(trans_df) -> int:
    """
    Count of Wire/ACH transactions in period A [2022-09-01, 2022-09-05] whose
    amount converted to USD (via Frankfurter API) is less than 1.

    Returns an integer with the result.
    """
    trans_sept_1st_df = trans_df[
        (trans_df["Timestamp"] >= "2022/09/01") & (trans_df["Timestamp"] < "2022/09/06")
    ]
    trans_wire_ach_df = trans_sept_1st_df[
        (trans_sept_1st_df["Payment Format"] == "Wire")
        | (trans_sept_1st_df["Payment Format"] == "ACH")
    ].copy()

    dates = trans_wire_ach_df["Timestamp"].str[:10].tolist()
    currencies = trans_wire_ach_df["Payment Currency"].tolist()
    amounts = trans_wire_ach_df["Amount Paid"].tolist()
    trans_wire_ach_df["USD Amount"] = [
        amt * _get_rates(d).get(c, 1.0) for amt, d, c in zip(amounts, dates, currencies)
    ]
    return trans_wire_ach_df[trans_wire_ach_df["USD Amount"] < 1.0].shape[0]


if __name__ == "__main__":
    main()
