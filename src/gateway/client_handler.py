import logging
from threading import Thread
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
        transactions_tx: MessageMiddlewareQueue,
        accounts_tx: MessageMiddlewareQueue,
    ):
        self.id = UUID()
        self.conn = conn
        self.transactions_tx = transactions_tx
        self.accounts_tx = accounts_tx
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
        # TODO: check msg integrity (correct variants)
        # recv transactions
        while True:
            msg = deserialize_message(self.conn.recv())

            self.transactions_tx.send(msg.serialize())

            if msg.type().value == MessageType.EOF.value:
                break

        # recv accounts
        while True:
            msg = deserialize_message(self.conn.recv())

            self.accounts_tx.send(msg.serialize())

            if msg.type().value == MessageType.EOF.value:
                break

            self.accounts_tx.send(msg.serialize())
