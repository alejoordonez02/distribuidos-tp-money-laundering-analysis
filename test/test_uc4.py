from collections import Counter
from dataclasses import dataclass

from scripts.cfg import (
    CLIENT_EXPECTED_RESPONSES_PATH,
    CLIENT_RESPONSES_PATH,
    NCLIENTS,
)


@dataclass
class Result:
    bank: str
    account: str

    def __hash__(self):
        return hash((self.bank, self.account))


def test_uc4():
    """
    Test UC4 for all clients.

    This test assumes that
    * the system has been ran and responses exist, and
    * the expected responses have already been generated.
    """
    for n in range(NCLIENTS):
        expected = Counter()
        got = Counter()

        with open(
            CLIENT_EXPECTED_RESPONSES_PATH + f"uc4_{n}.csv", "r"
        ) as expected_responses:
            expected_responses.readline()  # skip header
            while line := expected_responses.readline():
                expected[Result(*line.rstrip("\n").split(",")[1:])] += 1

        with open(CLIENT_RESPONSES_PATH + f"responses_{n}.csv", "r") as responses:
            while line := responses.readline():
                if "--- UC4 ---" in line:
                    break

            while line := responses.readline():
                if "--- UC" in line:
                    break

                got[
                    Result(
                        *line.rstrip("\n")
                        .replace(" ", "")
                        .replace("bank:", "")
                        .replace("account:", ",")
                        .split(",")
                    )
                ] += 1

        assert got == expected
