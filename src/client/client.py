import logging
import os
import time
from uuid import uuid4

from parser import Parser

from common.comms.connection import Connection
from common.comms.messages import EOF, Accounts, Transactions
from common.comms.messages.deserialize_message import Response
from common.data import Account, Transaction
from common.graceful_shutdown import setup_graceful_shutdown

# TODO: this should be dynamic (use some fin msg protocol)
NRESPONSES = int(os.getenv("NRESPONSES", "1"))
# TODO: no way this uuid can be here
TMP_CLIENT_ID = uuid4()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))


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
        setup_graceful_shutdown(self.stop)
        # TODO: esto lo dejo acá porque me trabé haciendo q se ejecute bien el script de healthcheck
        time.sleep(10)
        self._run()

    def stop(self):
        try:
            self.conn.close()
        except OSError:
            pass

    def _run(self):
        # read and send datasets in batches
        self._send_transactions_batched()
        self._send_eof()

        logging.info("sent transactions eof to server")

        self._send_accounts_batched()
        self._send_eof()

        logging.info("sent accounts eof to server")

        # receive and write responses
        logging.info("waiting for server responses")

        responses = self._receive_responses()
        self._write_responses(responses)

        logging.info("received server responses. Bye")

        self.conn.close()

    def _send_transactions_batched(self):
        """
        Read transactions from CSV in batches of BATCH_SIZE and send each batch to server.
        """
        with open(self.transactions_path, "r") as f:
            f.readline()  # skip header
            batch: list[Transaction] = []
            while line := f.readline():
                batch.append(self.transaction_parser.parse(line))
                if len(batch) == BATCH_SIZE:
                    self.conn.send(Transactions(TMP_CLIENT_ID, batch).serialize())
                    batch = []
            if batch:
                self.conn.send(Transactions(TMP_CLIENT_ID, batch).serialize())

    def _send_accounts_batched(self):
        """
        Read accounts from CSV in batches of BATCH_SIZE and send each batch to server.
        """
        with open(self.accounts_path, "r") as f:
            f.readline()  # skip header
            batch: list[Account] = []
            while line := f.readline():
                batch.append(self.account_parser.parse(line))
                if len(batch) == BATCH_SIZE:
                    self.conn.send(Accounts(TMP_CLIENT_ID, batch).serialize())
                    batch = []
            if batch:
                self.conn.send(Accounts(TMP_CLIENT_ID, batch).serialize())

    def _send_eof(self):
        self.conn.send(EOF(TMP_CLIENT_ID).serialize())

    def _receive_responses(self):
        responses = []

        for _ in range(NRESPONSES):
            response = Response.deserialize(self.conn.recv())
            logging.info(f"received server response:\n{response.body[:11]}")  # type: ignore[reportAttributeAccessIssue]
            responses.append(response.body)  # type: ignore[reportAttributeAccessIssue]

        return responses

    def _write_responses(self, responses):
        with open(self.responses_path, "w") as file:
            for r in responses:
                file.write(r)
