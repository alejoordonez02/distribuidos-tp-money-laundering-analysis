# This analysis was taken from
# `https://www.kaggle.com/code/pablodroca/money-laundering-analysis`,
# source was only modified so that it produces files with input and expected output
# for each client.

import json
import urllib.request

import pandas as pd  # pyright: ignore    no me toma pandas :(
from cfg import (
    ACCOUNTS_PATH,
    ACCOUNTS_SAMPLE_SIZE,
    CLIENT_DATASETS_PATH,
    CLIENT_EXPECTED_RESPONSES_PATH,
    NCLIENTS,
    TRANSACTIONS_PATH,
    TRANSACTIONS_SAMPLE_SIZE,
)


def main():
    """
    Generate the input and expected output for each client.
    """
    # same accounts dataset for all clients
    accounts_df = pd.read_csv(ACCOUNTS_PATH)
    if ACCOUNTS_SAMPLE_SIZE is not None:
        accounts_df = accounts_df.sample(ACCOUNTS_SAMPLE_SIZE)

    for n in range(NCLIENTS):
        trans_df = gen_sampled_dataframe(
            TRANSACTIONS_SAMPLE_SIZE,
            TRANSACTIONS_PATH,
            CLIENT_DATASETS_PATH + f"transactions_{n}.csv",
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
):
    """
    Samples a dataset, writes it to its corresponding path and returns it.
    """
    sampled_df = pd.read_csv(dataframe_path).sample(sample_size)
    sampled_df.to_csv(sampled_path)

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
    low_profile_transactions = low_profile_transactions[
        ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"]
    ]

    return low_profile_transactions


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
    ).rename(
        columns={
            "Amount Paid_x": "Amount Paid",
            "Amount Paid_y": "AVG",
        }
    )
    lower_trans_usd_sept_2nd_with_avg_df = trans_usd_sept_2nd_with_avg_df[
        trans_usd_sept_2nd_with_avg_df["Amount Paid"]
        < trans_usd_sept_2nd_with_avg_df["AVG"] * 0.01
    ]

    return lower_trans_usd_sept_2nd_with_avg_df[
        ["From Bank", "Account", "Payment Format", "Amount Paid"]
    ]


def gen_uc4_results(trans_df):
    """
    Accounts that match the scatter-gather pattern and where the source account has
    transferred to more than 5 distinct accounts.

    Returns a `DataFrame` with the results.
    """

    def filter_function(x):
        unique_account_size = x.groupby(["To Bank", "Account.1"]).size().size
        return unique_account_size > 5 and unique_account_size < 10

    trans_usd_df = trans_df[trans_df["Payment Currency"] == "US Dollar"]

    trans_usd_sept_1st_df = trans_usd_df[
        (trans_usd_df["Timestamp"] >= "2022/09/01")
        & (trans_usd_df["Timestamp"] <= "2022/09/06")
    ]

    ranged_trans_usd_sept_df = trans_usd_sept_1st_df.groupby(
        ["From Bank", "Account"]
    ).filter(filter_function)

    accounts_df = ranged_trans_usd_sept_df[
        ["From Bank", "Account", "To Bank", "Account.1"]
    ]
    account_pairs_df = accounts_df.merge(
        accounts_df, left_on=["To Bank", "Account.1"], right_on=["From Bank", "Account"]
    ).rename(
        columns={
            "From Bank_x": "From Bank",
            "Account_x": "From Account",
            "To Bank_y": "To Bank",
            "Account.1_y": "To Account",
        }
    )
    account_pairs_df = account_pairs_df[
        (account_pairs_df["From Bank"] != account_pairs_df["To Bank"])
        | (account_pairs_df["From Account"] != account_pairs_df["To Account"])
    ]
    account_pairs_df = account_pairs_df.groupby(
        ["From Bank", "From Account", "To Bank", "To Account"], as_index=False
    ).size()
    account_pairs_df = account_pairs_df[(account_pairs_df["size"] > 5)]

    from_account_pairs_df = account_pairs_df[["From Bank", "From Account"]].rename(
        columns={"From Bank": "Bank", "From Account": "Account"}
    )
    to_account_pairs_df = account_pairs_df[["To Bank", "To Account"]].rename(
        columns={"To Bank": "Bank", "To Account": "Account"}
    )
    unique_accounts = pd.concat(
        [from_account_pairs_df, to_account_pairs_df]
    ).drop_duplicates()

    return unique_accounts


_CURRENCY_ISO = {
    "Australian Dollar": "AUD",
    "Brazil Real": "BRL",
    "Canadian Dollar": "CAD",
    "Euro": "EUR",
    "Mexican Peso": "MXN",
    "Rupee": "INR",
    "Shekel": "ILS",
    "Swiss Franc": "CHF",
    "UK Pound": "GBP",
    "US Dollar": "USD",
    "Yen": "JPY",
    "Yuan": "CNY",
}

_USD_FALLBACK = {
    "Bitcoin": 78.33,
    "Ruble": 0.01,
    "Saudi Riyal": 0.27,
}


def _fetch_usd_rates(date_dash: str) -> dict[str, float]:
    url = f"https://api.frankfurter.app/{date_dash}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read())
    eur_rates = data["rates"]
    usd_per_eur = eur_rates["USD"]
    rates: dict[str, float] = {"US Dollar": 1.0, "Euro": usd_per_eur}
    for name, iso in _CURRENCY_ISO.items():
        if iso in ("USD", "EUR"):
            continue
        if iso in eur_rates:
            rates[name] = usd_per_eur / eur_rates[iso]
    rates.update(_USD_FALLBACK)
    return rates


def gen_uc5_results(trans_df) -> int:
    """
    Count of transactions of period [2022-09-01, 2022-09-05] with type Wire or ACH,
    having converted amount in USD less than 1. Conversion rates fetched from
    the Frankfurter API (api.frankfurter.app) for each transaction date.

    Returns an integer with the result.
    """
    trans_sept_1st_df = trans_df[
        (trans_df["Timestamp"] >= "2022/09/01")
        & (trans_df["Timestamp"] <= "2022/09/06")
    ]
    trans_wire_ach_df = trans_sept_1st_df[
        (trans_sept_1st_df["Payment Format"] == "Wire")
        | (trans_sept_1st_df["Payment Format"] == "ACH")
    ].copy()

    # Fetch rates for each unique date (at most 5 calls for period A)
    rate_cache: dict[str, dict[str, float]] = {}
    for date_slash in trans_wire_ach_df["Timestamp"].str[:10].unique():
        date_dash = date_slash.replace("/", "-")
        rate_cache[date_slash] = _fetch_usd_rates(date_dash)

    trans_wire_ach_df["USD Amount"] = trans_wire_ach_df.apply(
        lambda row: row["Amount Paid"]
        * rate_cache[row["Timestamp"][:10]].get(row["Payment Currency"], 1.0),
        axis=1,
    )

    return trans_wire_ach_df[trans_wire_ach_df["USD Amount"] < 1.0].shape[0]


if __name__ == "__main__":
    main()
