import logging

from common.comms.connection import Connection
from common.comms.messages import (
    Message,
    MessageType,
    deserialize_message,
)
from common.comms.middleware import MessageMiddlewareQueue


class ClientHandler:
    def __init__(
        self,
        conn: Connection,
        transactions_tx: MessageMiddlewareQueue,
        accounts_tx: MessageMiddlewareQueue,
    ):
        self.conn = conn
        self.server_tx = transactions_tx
        self.transactions_tx = transactions_tx
        self.accounts_tx = accounts_tx

    def start(self):
        self._run()

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

            self.transactions_tx.send(msg.serialize())

            if msg.type().value == MessageType.EOF.value:
                break

            self.accounts_tx.send(msg.serialize())
