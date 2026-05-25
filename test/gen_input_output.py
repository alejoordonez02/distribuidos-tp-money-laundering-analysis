# This analysis was taken from
# `https://www.kaggle.com/code/pablodroca/money-laundering-analysis`,
# source was only modified so that it produces files with input and expected output
# for each client.

from datetime import date

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

from common.conversion import FrankfurterConversionAPI

RANDOM_SEED = 2026

_conversion_api = FrankfurterConversionAPI()
_rate_cache: dict[date, dict[str, float]] = {}


def _get_rates(date_slash: str) -> dict[str, float]:
    day = date.fromisoformat(date_slash.replace("/", "-"))
    if day not in _rate_cache:
        _rate_cache[day] = _conversion_api.get_rates(day)
    return _rate_cache[day]


def main():
    """
    Generate the input and expected output for each client.
    """
    rng = np.random.default_rng(seed=RANDOM_SEED)

    # same accounts dataset for all clients
    accounts_df = pd.read_csv(ACCOUNTS_PATH)
    if ACCOUNTS_SAMPLE_SIZE is not None:
        accounts_df = gen_sampled_dataframe(
            ACCOUNTS_SAMPLE_SIZE,
            ACCOUNTS_PATH,
            CLIENT_DATASETS_PATH + "accounts.csv",
            rng,
        )

    for n in range(NCLIENTS):
        trans_df = gen_sampled_dataframe(
            TRANSACTIONS_SAMPLE_SIZE,
            TRANSACTIONS_PATH,
            CLIENT_DATASETS_PATH + f"transactions_{n}.csv",
            rng,
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


def gen_sampled_dataframe(
    sample_size: int | None,
    dataframe_path: str,
    sampled_path: str,
    rng: np.random.Generator,
):
    """
    Samples a dataset, writes it to its corresponding path and returns it.
    """
    sampled_df = pd.read_csv(dataframe_path)
    if sample_size:
        sampled_df = sampled_df.sample(sample_size, random_state=rng)

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
    uc1_results = gen_uc1_results(trans_df)
    uc1_results.to_csv(uc1_results_path)

    uc2_results = gen_uc2_results(trans_df, accounts_df)
    uc2_results.to_csv(uc2_results_path)

    uc3_results = gen_uc3_results(trans_df)
    uc3_results.to_csv(uc3_results_path)

    uc4_results = gen_uc4_results(trans_df)
    uc4_results.to_csv(uc4_results_path)

    uc5_results = gen_uc5_results(trans_df)
    with open(uc5_results_path, "w") as f:
        f.write(str(uc5_results))


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
        & (trans_usd_df["Timestamp"] <= "2022/09/15")
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

    # DIFERENCIA CON EL NOTEBOOK: el notebook pre-filtraba cuentas origen con > 5
    # destinos distintos antes del join. Ese pre-filtro no está en el enunciado —
    # es una consecuencia implícita de la condición final (si un par (A,C) tiene >= 5
    # intermediarios distintos, A necesariamente mandó a >= 5 cuentas). Se omite para
    # no agregar una restricción que el enunciado no menciona explícitamente.
    edges_df = trans_usd_sept_1st_df[["From Bank", "Account", "To Bank", "Account.1"]]

    # Self-join: construir cadenas A → B → C (una sola cuenta de separación)
    chains_df = edges_df.merge(
        edges_df,
        left_on=["To Bank", "Account.1"],
        right_on=["From Bank", "Account"],
    ).rename(
        columns={
            "From Bank_x": "From Bank",
            "Account_x": "From Account",
            "To Bank_y": "To Bank",
            "Account.1_y": "To Account",
        }
    )

    # Eliminar pares donde A == C
    chains_df = chains_df[
        (chains_df["From Bank"] != chains_df["To Bank"])
        | (chains_df["From Account"] != chains_df["To Account"])
    ]

    # DIFERENCIA CON EL NOTEBOOK: el notebook usaba .size() sobre el groupby, que
    # cuenta filas del join (combinaciones de transacciones A→B y B→C). Si A mandó
    # 3 transacciones a B y B mandó 4 a C, eso suma 12 — no 1 cuenta intermediaria.
    # El enunciado dice "5 cuentas distintas", por lo que la unidad correcta son
    # cuentas B distintas, no transacciones. Usamos nunique() sobre el identificador
    # de B (To Bank_x + Account.1_x) para contar intermediarios distintos por par (A,C).
    chains_df["_B"] = (
        chains_df["To Bank_x"].astype(str) + "-" + chains_df["Account.1_x"].astype(str)
    )
    intermediary_counts = (
        chains_df.groupby(["From Bank", "From Account", "To Bank", "To Account"])["_B"]
        .nunique()
        .reset_index(name="intermediaries")
    )

    # DIFERENCIA CON EL NOTEBOOK: el notebook filtraba con > 5 (estrictamente mayor).
    # El enunciado dice "hacia 5 cuentas distintas", donde 5 es el mínimo que cumple
    # el patrón, no el que se descarta. Se usa >= 5.
    valid_pairs = intermediary_counts[intermediary_counts["intermediaries"] >= 5]

    from_accounts = valid_pairs[["From Bank", "From Account"]].rename(
        columns={"From Bank": "Bank", "From Account": "Account"}
    )
    to_accounts = valid_pairs[["To Bank", "To Account"]].rename(
        columns={"To Bank": "Bank", "To Account": "Account"}
    )
    return pd.concat([from_accounts, to_accounts]).drop_duplicates()


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

    trans_wire_ach_df["USD Amount"] = trans_wire_ach_df.apply(
        lambda row: (
            row["Amount Paid"]
            * _get_rates(row["Timestamp"][:10]).get(row["Payment Currency"], 1.0)
        ),
        axis=1,
    )
    return trans_wire_ach_df[trans_wire_ach_df["USD Amount"] < 1.0].shape[0]


if __name__ == "__main__":
    main()
