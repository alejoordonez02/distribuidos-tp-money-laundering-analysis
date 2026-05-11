from socket import socket

from common.comms.messages import Message
from common.comms.messages.eof import EOF
from common.comms.middleware import MessageMiddlewareQueue

BUF_SIZE = 1024


class ClientHandler:
    def __init__(
        self,
        skt: socket,
        transactions_tx: MessageMiddlewareQueue,
        accounts_tx: MessageMiddlewareQueue,
    ):
        self.skt = skt
        self.server_tx = transactions_tx
        self.transactions_tx = transactions_tx
        self.accounts_tx = accounts_tx

    def start(self):
        self._run()

    def send(self, msg: Message):
        self.skt.send(msg.serialize())

    def _run(self):
        # recv transactions
        while True:
            msg = Message.deserialize(self.skt.recv(BUF_SIZE))
            # TODO: check msg integrity
            if msg.type() == EOF:
                break

            self.transactions_tx.send(msg.serialize())

        # recv accounts
        while True:
            msg = Message.deserialize(self.skt.recv(BUF_SIZE))
            if msg.type() == EOF:
                break

            # TODO: check msg integrity
            self.accounts_tx.send(msg.serialize())
