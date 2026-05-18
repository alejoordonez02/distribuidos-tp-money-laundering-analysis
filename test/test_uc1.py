from dataclasses import dataclass

from .cfg import (
    CLIENT_EXPECTED_RESPONSES_PATH,
    CLIENT_RESPONSES_PATH,
    NCLIENTS,
)


@dataclass
class Result:
    from_bank: str
    from_account: str
    to_bank: str
    to_account: str
    amount: str

    def __hash__(self):
        return hash(
            (
                self.from_bank,
                self.from_account,
                self.to_bank,
                self.to_account,
                self.amount,
            )
        )


# TODO: las condiciones de corte que puse ("--- UC1 ---" o "---") están horribles,
#       quizás se podría reutilizar el formatter que usa join...
# TODO: deshardcodear los nombres de los archivos
def test_uc1():
    """
    Test UC1 for all clients.

    This test assumes that
    * the system has been ran and responses exist, and
    * the expected responses have already been generated.
    """
    for n in range(NCLIENTS):
        expected = set()
        got = set()

        with open(
            CLIENT_EXPECTED_RESPONSES_PATH + f"uc1_{n}.csv", "r"
        ) as expected_responses:
            expected_responses.readline()  # skip header
            while line := expected_responses.readline():
                expected.add(Result(*line.rstrip("\n").split(",")[1:]))

        with open(CLIENT_RESPONSES_PATH + f"responses_{n}.csv", "r") as responses:
            while line := responses.readline():
                if "--- UC1 ---" in line:
                    break

            # origin: 03402-80021DAD0      destination: 03402-80021DAD0      amount: 1858.96
            while line := responses.readline():
                if "--- UC" in line:
                    break

                got.add(
                    Result(
                        *line.replace("origin:", ",")
                        .replace("-", ",")
                        .replace("destination:", ",")
                        .replace("amount:", ",")
                        .replace(" ", "")
                        .rstrip("\n")
                        .split(",")[1:]
                    )
                )

        assert got == expected
