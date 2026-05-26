import logging
from threading import Thread
from typing import Callable
from uuid import uuid4

from common.comms.connection import Connection
from common.comms.messages import (
    Message,
    MessageType,
    deserialize_message,
)
from common.comms.messages.errors import UnexpectedMessageError
from common.comms.middleware import MOMQueue

UUID = uuid4


class ClientHandler:
    def __init__(
        self,
        conn: Connection,
        trans_tx_factory: Callable[[], MOMQueue],
        accs_tx_factory: Callable[[], MOMQueue],
    ):
        self.id = UUID()
        self.conn = conn
        self.trans_tx_factory = trans_tx_factory
        self.accs_tx_factory = accs_tx_factory
        self.handle: Thread

    def start(self):
        """
        Starts a thread for receiving and redirecting its client's
        transactions and accounts datasets.
        """
        self.handle = Thread(target=self._run)
        self.handle.start()

    def send(self, msg: Message):
        self.conn.send(msg.serialize())

    def _run(self):
        self._handle_transactions()
        self._handle_accounts()
        logging.info("finished sending all client's data")

    def _handle_transactions(self):
        transactions_tx = self.trans_tx_factory()
        count = 0

        while True:
            msg = deserialize_message(self.conn.recv())
            logging.debug(f"received message from client: {msg.__dict__}")

            match msg.type():
                case MessageType.EOF:
                    msg.client_id = self.id
                    msg.expected_count = count  # type: ignore[reportAttributeAccessIssue]
                    transactions_tx.send(msg.serialize())
                    break
                case MessageType.TRANSACTIONS:
                    msg.client_id = self.id
                    transactions_tx.send(msg.serialize())
                    count += 1
                case _:
                    raise UnexpectedMessageError(
                        "client handler received unexpected msg: {msg.__dict__}"
                    )

    def _handle_accounts(self):
        accounts_tx = self.accs_tx_factory()
        count = 0

        while True:
            msg = deserialize_message(self.conn.recv())
            logging.debug(f"received message from client: {msg.__dict__}")

            match msg.type():
                case MessageType.EOF:
                    msg.client_id = self.id
                    msg.expected_count = count  # type: ignore[reportAttributeAccessIssue]
                    accounts_tx.send(msg.serialize())
                    break
                case MessageType.ACCOUNTS:
                    msg.client_id = self.id
                    accounts_tx.send(msg.serialize())
                    count += 1
                case _:
                    raise UnexpectedMessageError(
                        "client handler received unexpected msg: {msg.__dict__}"
                    )
