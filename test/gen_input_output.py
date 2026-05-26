# This analysis was taken from
# `https://www.kaggle.com/code/pablodroca/money-laundering-analysis`,
# source was only modified so that it produces files with input and expected output
# for each client.

import gc
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
    TRANSACTIONS_SAMPLE_SIZE,
)
from pandas.core.generic import DtypeArg # type: ignore

from common.conversion import FrankfurterConversionAPI

RANDOM_SEED = 2026

_CHUNK_SIZE = 50_000

# Optimized dtypes: categories for low-cardinality strings, int32/int8 for integers.
# Amount Paid stays float64 to preserve precision in expected output comparisons.
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
    """
    _log("=== gen_input_output START ===")
    _log(
        f"NCLIENTS={NCLIENTS}, TRANSACTIONS_SAMPLE_SIZE={TRANSACTIONS_SAMPLE_SIZE}, ACCOUNTS_SAMPLE_SIZE={ACCOUNTS_SAMPLE_SIZE}"
    )
    rng = np.random.default_rng(seed=RANDOM_SEED)

    # same accounts dataset for all clients
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

    for n in range(NCLIENTS):
        _log(f"--- Client {n + 1}/{NCLIENTS} ---")
        _log(f"Sampling transactions from {TRANSACTIONS_PATH} ...")
        trans_df = gen_sampled_dataframe(
            TRANSACTIONS_SAMPLE_SIZE,
            TRANSACTIONS_PATH,
            CLIENT_DATASETS_PATH + f"transactions_{n}.csv",
            rng,
            dtype=_TRANS_DTYPE,
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


def _chunked_sample(
    path: str,
    n: int,
    rng: np.random.Generator,
    dtype: DtypeArg | None = None,
) -> pd.DataFrame:
    """Read CSV in chunks, returning n randomly sampled rows without loading the full file."""
    with open(path, "rb") as f:
        n_total = sum(1 for _ in f) - 1  # exclude header

    if n >= n_total:
        return pd.read_csv(path, dtype=dtype)

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
):
    """
    Samples a dataset, writes it to its corresponding path and returns it.
    When sample_size is None, loads the full file with dtype optimization.
    When sample_size is set, uses chunked reading to avoid loading the full file.
    """
    if sample_size is None:
        sampled_df = pd.read_csv(dataframe_path, dtype=dtype)
    else:
        sampled_df = _chunked_sample(dataframe_path, sample_size, rng, dtype=dtype)

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
    # Each bank_id maps to exactly one bank_name; deduplicate before merging
    # to avoid producing one row per account in the bank (many-to-many explosion)
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
    from collections import defaultdict

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
    for a, b in edges:
        succs_of[a].add(b)

    pair_intermediaries: dict[tuple[int, int], set[int]] = defaultdict(set)
    for a, b in edges:
        for c in succs_of.get(b, set()):
            if a != c:
                pair_intermediaries[(a, c)].add(b)

    accounts: set[tuple] = set()
    for (a, c), bs in pair_intermediaries.items():
        if len(bs) >= 5:
            accounts.add(id_to_node[a])
            accounts.add(id_to_node[c])

    return pd.DataFrame(
        [(bank, acc) for bank, acc in accounts],
        columns=["Bank", "Account"],
    )


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
