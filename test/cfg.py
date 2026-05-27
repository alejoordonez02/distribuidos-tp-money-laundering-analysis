TRANSACTIONS_PATH = "datasets/LI-Small_Trans.csv"
ACCOUNTS_PATH = "datasets/LI-Small_accounts.csv"
ACCOUNTS_SAMPLE_SIZE = None  # whole dataset

NCLIENTS = 8
TRANSACTIONS_SAMPLE_FRAC: float | None = 0.5  # None = whole dataset per client; float = total fraction, split evenly across clients
CLIENT_DATASETS_PATH = "datasets/"
CLIENT_EXPECTED_RESPONSES_PATH = "test/expected_responses/"

CLIENT_RESPONSES_PATH = "responses/"
