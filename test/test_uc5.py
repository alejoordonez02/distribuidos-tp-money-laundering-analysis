from scripts.cfg import (
    CLIENT_EXPECTED_RESPONSES_PATH,
    CLIENT_RESPONSES_PATH,
    NCLIENTS,
)


def test_uc5():
    """
    Test UC5 for all clients.

    This test assumes that
    * the system has been ran and responses exist, and
    * the expected responses have already been generated.
    """
    for n in range(NCLIENTS):
        with open(CLIENT_EXPECTED_RESPONSES_PATH + f"uc5_{n}.csv", "r") as f:
            expected = int(f.read().strip())

        got = None
        with open(CLIENT_RESPONSES_PATH + f"responses_{n}.csv", "r") as responses:
            while line := responses.readline():
                if "--- UC5 ---" in line:
                    break

            while line := responses.readline():
                if "--- UC" in line:
                    break
                if line.startswith("count:"):
                    got = int(line.split(":")[1].strip())

        assert got == expected
