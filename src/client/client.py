import logging
import time
from uuid import uuid4

from parser import Parser

from common.comms.connection import Connection
from common.comms.messages import EOF, Accounts, Transactions
from common.comms.messages.deserialize_message import Response
from common.data import Account, Transaction

# TODO: this should be dynamic (use some fin msg protocol)
NRESPONSES = 1
# TODO: no way this uuid can be here
TMP_CLIENT_ID = uuid4()


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
        time.sleep(10)
        self._run()

    def _run(self):
        # read datasets
        transactions = self._read_transactions()
        accounts = self._read_accounts()

        # send data
        self._send_transactions(transactions)
        self._send_eof()

        logging.info("sent transactions eof to server")

        self._send_accounts(accounts)
        self._send_eof()

        logging.info("sent accounts eof to server")

        # receive and write responses
        logging.info("waiting for server responses")

        responses = self._receive_responses()
        self._write_responses(responses)

        logging.info("received server responses. Bye")

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
        msg = Transactions(TMP_CLIENT_ID, transactions)
        self.conn.send(msg.serialize())

    def _send_accounts(self, accounts: list[Account]):
        """
        Send accounts batch to server.
        """
        msg = Accounts(TMP_CLIENT_ID, accounts)
        self.conn.send(msg.serialize())

    def _send_eof(self):
        self.conn.send(EOF(TMP_CLIENT_ID).serialize())

    def _receive_responses(self):
        responses = []

        for _ in range(NRESPONSES):
            response = Response.deserialize(self.conn.recv())
            responses.append(response.body)  # type: ignore[reportAttributeAccessIssue]

        return responses

    def _write_responses(self, responses):
        with open(self.responses_path, "w") as file:
            for r in responses:
                file.write(r)
