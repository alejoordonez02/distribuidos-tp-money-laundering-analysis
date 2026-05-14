import time
from enum import Enum

from parser import Parser

from common.comms.connection import Connection
from common.comms.messages import EOF, Account, Transaction


class ResponseType(Enum):
    FIN = 0


def get_response_type(response: bytes):
    return response[0]


class Client:
    def __init__(
        self,
        conn: Connection,
        transactions_path: str,
        accounts_path: str,
        responses_path: str,
        transaction_parser: Parser,
        account_parser: Parser,
    ):
        self.conn = conn
        self.transactions_path = transactions_path
        self.accounts_path = accounts_path
        self.responses_path = responses_path
        self.transaction_parser = transaction_parser
        self.account_parser = account_parser

    def start(self):
        # TODO: esto lo dejo acá porque me trabé haciendo q se ejecute bien el script de healthcheck
        time.sleep(1)
        self._run()

    def _run(self):
        # read datasets
        transactions = self._read_transactions()
        accounts = self._read_accounts()

        # send data
        self._send_transactions(transactions)
        self._send_eof()
        self._send_accounts(accounts)
        self._send_eof()

        # receive and write responses
        responses = self._receive_responses()
        self._write_responses(responses)

        self.conn.close()

    # TODO: read batches rather than the whole thing
    def _read_transactions(self) -> list[Transaction]:
        transactions_batch = []

        with open(self.transactions_path, "r") as transactions:
            transactions.readline()  # ignore header

            while line := transactions.readline():
                transaction = self.transaction_parser.parse(line)
                transactions_batch.append(transaction)

        return transactions_batch

    # TODO: read batches rather than the whole thing
    def _read_accounts(self) -> list[Account]:
        accounts_batch = []

        with open(self.accounts_path, "r") as accounts:
            accounts.readline()  # ignore header

            while line := accounts.readline():
                account = self.account_parser.parse(line)
                accounts_batch.append(account)

        return accounts_batch

    def _send_transactions(self, transactions: list[Transaction]):
        """
        Send transaction batch to server.
        """
        for t in transactions:
            self.conn.send(t.serialize())

    def _send_accounts(self, accounts: list[Account]):
        """
        Send accounts batch to server.
        """
        for a in accounts:
            self.conn.send(a.serialize())

    def _send_eof(self):
        self.conn.send(EOF().serialize())

    def _receive_responses(self):
        responses = []

        while True:
            response = self.conn.recv()
            responses.append(response.decode())
            if get_response_type(response) == ResponseType.FIN.value:
                break

        return responses

    def _write_responses(self, responses):
        with open(self.responses_path, "w") as file:
            for r in responses:
                file.write(r)
