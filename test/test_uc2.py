from dataclasses import dataclass

from .cfg import (
    CLIENT_EXPECTED_RESPONSES_PATH,
    CLIENT_RESPONSES_PATH,
    NCLIENTS,
)


@dataclass
class Result:
    bank_id: str
    account: str
    bank_name: str
    amount: float

    def __hash__(self):
        return hash((self.bank_id, self.account, self.bank_name, round(self.amount, 2)))

    def __eq__(self, other):
        return (
            self.bank_id == other.bank_id
            and self.account == other.account
            and self.bank_name == other.bank_name
            and round(self.amount, 2) == round(other.amount, 2)
        )


def test_uc2():
    """
    Test UC2 for all clients.

    This test assumes that
    * the system has been ran and responses exist, and
    * the expected responses have already been generated.
    """
    for n in range(NCLIENTS):
        expected = set()
        got = set()

        with open(
            CLIENT_EXPECTED_RESPONSES_PATH + f"uc2_{n}.csv", "r"
        ) as expected_responses:
            expected_responses.readline()  # skip header
            while line := expected_responses.readline():
                # CSV: idx,From Bank,Account,Bank Name,Amount Paid
                # Split at most 4 times to handle bank names with commas
                parts = line.rstrip("\n").split(",", 4)
                _, bank_id, account, bank_name, amount = parts
                expected.add(Result(bank_id, account, bank_name, float(amount)))

        with open(CLIENT_RESPONSES_PATH + f"responses_{n}.csv", "r") as responses:
            while line := responses.readline():
                if "--- UC2 ---" in line:
                    break

            # format: bank_id|account|bank_name|amount
            while line := responses.readline():
                if "--- UC" in line:
                    break
                bank_id, account, bank_name, amount = line.strip().split("|")
                got.add(Result(bank_id, account, bank_name, float(amount)))

        assert got == expected
