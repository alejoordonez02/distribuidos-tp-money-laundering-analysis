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
        transactions and accounts datasets. Once all data is sent to
        the next controller, the thread is joined.
        """
        self.handle = Thread(target=self._run)
        self.handle.join()

    def send(self, msg: Message):
        self.conn.send(msg.serialize())

    def _run(self):
        # recv transactions
        transactions_tx = self.trans_tx_factory()
        while True:
            msg = deserialize_message(self.conn.recv())
            logging.debug(f"received message from client: {msg.__dict__}")
            # TODO: check msg integrity (correct variants)
            transactions_tx.send(msg.serialize())

            if msg.type().value == MessageType.EOF.value:
                break

        # recv accounts
        accounts_tx = self.accs_tx_factory()
        while True:
            msg = deserialize_message(self.conn.recv())
            logging.debug(f"received message from client: {msg.__dict__}")
            # TODO: check msg integrity (correct variants)
            accounts_tx.send(msg.serialize())

            if msg.type().value == MessageType.EOF.value:
                break

        logging.info("finished sending all client's data")
