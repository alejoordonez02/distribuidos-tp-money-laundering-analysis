import os
import time
from enum import Enum
from socket import AF_INET, SOCK_STREAM, socket

TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
ACCOUNTS_PATH = os.environ["ACCOUNTS_PATH"]
RESPONSES_PATH = os.environ["RESPONSES_PATH"]
GATEWAY_HOST = os.environ["GATEWAY_HOST"]
GATEWAY_PORT = os.environ["GATEWAY_PORT"]

BUF_SIZE = 1024


class ResponseType(Enum):
    FIN = 0


def get_response_type(response: bytes):
    return response[0]


def main():
    # TODO: esto lo dejo acá porque me trabé haciendo q se ejecute bien el script de healthcheck
    time.sleep(1)
    skt = socket(AF_INET, SOCK_STREAM)
    skt.connect((GATEWAY_HOST, int(GATEWAY_PORT)))

    transactions_batch = []
    with open(TRANSACTIONS_PATH, "r") as transactions:
        line = transactions.readline()
        transactions_batch.append(line)

    accounts_batch = []
    with open(ACCOUNTS_PATH, "r") as accounts:
        line = accounts.readline()
        accounts_batch.append(line)

    for t in transactions_batch:
        skt.sendall(t.encode())

    for t in accounts_batch:
        skt.sendall(t.encode())

    responses = []
    while True:
        response = skt.recv(BUF_SIZE)
        responses.append(response.decode())
        if get_response_type(response) == ResponseType.FIN.value:
            break

    skt.close()

    with open(RESPONSES_PATH, "w") as file:
        for r in responses:
            file.write(r)


if __name__ == "__main__":
    main()
