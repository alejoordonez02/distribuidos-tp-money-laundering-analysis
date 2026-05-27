TRANSACTIONS_PATH = "datasets/LI-Small_Trans.csv"
ACCOUNTS_PATH = "datasets/LI-Small_accounts.csv"
ACCOUNTS_SAMPLE_SIZE = None  # whole dataset

NCLIENTS = 8
TRANSACTIONS_SAMPLE_FRAC: float = 0.5  # fraction of the dataset each client gets independently; 1.0 = whole dataset, 1/NCLIENTS = equitable split
CLIENT_DATASETS_PATH = "datasets/"
CLIENT_EXPECTED_RESPONSES_PATH = "test/expected_responses/"

CLIENT_RESPONSES_PATH = "responses/"
