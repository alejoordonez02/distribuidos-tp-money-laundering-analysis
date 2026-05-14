import os
from socket import AF_INET, SOCK_STREAM, socket

from parser import AccountParser, TransactionParser

from client import Client  # type: ignore
from common.comms.connection import Connection

TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
ACCOUNTS_PATH = os.environ["ACCOUNTS_PATH"]
RESPONSES_PATH = os.environ["RESPONSES_PATH"]
GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]


def main():
    skt = socket(AF_INET, SOCK_STREAM)
    skt.connect((GATEWAY_HOST, int(GATEWAY_PORT)))
    conn = Connection(skt)

    transaction_parser = TransactionParser()
    account_parser = AccountParser()

    client = Client(
        conn,
        TRANSACTIONS_PATH,
        ACCOUNTS_PATH,
        RESPONSES_PATH,
        transaction_parser,
        account_parser,
    )
    client.start()


if __name__ == "__main__":
    main()
