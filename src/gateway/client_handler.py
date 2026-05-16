import logging
from threading import Thread
from typing import Callable
from uuid import uuid4

from common.comms.connection import Connection
from common.comms.messages import (
    EOF,
    Message,
    MessageType,
    deserialize_message,
)
from common.comms.middleware import MessageMiddlewareQueue

UUID = uuid4


class ClientHandler:
    def __init__(
        self,
        conn: Connection,
        trans_tx_factory: Callable[[], MessageMiddlewareQueue],
        accs_tx_factory: Callable[[], MessageMiddlewareQueue],
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
        # TODO: maybe have a different eof msg for the external protocol
        # recv transactions
        transactions_tx = self.trans_tx_factory()
        while True:
            msg = deserialize_message(self.conn.recv())
            logging.debug(f"received message from client: {msg.__dict__}")
            # TODO: check msg integrity (correct variants)
            if msg.type() == MessageType.EOF:
                # msg.client_id = self.id  # type: ignore[reportAttributeAccessIssue]
                transactions_tx.send(EOF(self.id).serialize())
                break

            transactions_tx.send(msg.serialize())

        # recv accounts
        accounts_tx = self.accs_tx_factory()
        while True:
            msg = deserialize_message(self.conn.recv())
            logging.debug(f"received message from client: {msg.__dict__}")
            # TODO: check msg integrity (correct variants)
            if msg.type() == MessageType.EOF:
                # msg.client_id = self.id  # type: ignore[reportAttributeAccessIssue]
                accounts_tx.send(EOF(self.id).serialize())
                break

            accounts_tx.send(msg.serialize())

        logging.info("finished sending all client's data")
