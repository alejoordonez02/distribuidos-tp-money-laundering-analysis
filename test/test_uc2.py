import re
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

            # format: bank_id: {:<20} account: {:<20} bank_name: {:<30} amount: {}
            while line := responses.readline():
                if "--- UC" in line:
                    break
                m = re.match(r"bank_id: (\S+)\s+account: (\S+)\s+bank_name: (.*?)\s+amount: (\S+)$", line.strip())
                bank_id, account, bank_name, amount = m.group(1), m.group(2), m.group(3), m.group(4)
                got.add(Result(bank_id, account, bank_name, float(amount)))

        assert got == expected
