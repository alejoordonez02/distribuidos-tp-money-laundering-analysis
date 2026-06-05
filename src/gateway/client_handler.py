import logging
from threading import Thread
from typing import TYPE_CHECKING, Callable
from uuid import uuid4

from common.comms.connection import Connection
from common.comms.messages import Hello, HelloAck, Message, MessageType
from common.comms.middleware import MOMQueue

if TYPE_CHECKING:
    from client_monitor import ClientMonitor


def _peek_type(raw: bytes) -> int:
    """The first byte of a serialized message is its type (1-byte prefix), so the
    gateway can route a batch without unpacking its msgpack payload."""
    return raw[0]


class ClientHandler:
    def __init__(
        self,
        conn: Connection,
        clients: "ClientMonitor",
        trans_tx_factory: Callable[[], MOMQueue],
        accs_tx_factory: Callable[[], MOMQueue],
    ):
        self.id = uuid4()
        self.conn = conn
        self.clients = clients
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
        # Handshake: the client sends Hello, the gateway MINTS the client_id and
        # returns it (HelloAck). The client then stamps that id on every message,
        # so the gateway keeps forwarding data raw (no per-batch deserialize) while
        # owning the id namespace — no collisions, no client-chosen ids. The
        # passthrough lets a client that parses in parallel avoid bottlenecking on
        # a single gateway core.
        Hello.deserialize(self.conn.recv())
        self.id = uuid4()
        self.clients.add(self)
        self.conn.send(HelloAck(self.id).serialize())

        self._forward_until_eof(self.trans_tx_factory(), MessageType.TRANSACTIONS)
        self._forward_until_eof(self.accs_tx_factory(), MessageType.ACCOUNTS)
        logging.info("finished forwarding all client's data")

    def _forward_until_eof(self, tx: MOMQueue, data_type: MessageType):
        while True:
            raw = self.conn.recv()
            if not raw:
                logging.error("client connection closed before EOF")
                return
            t = _peek_type(raw)
            if t == MessageType.EOF:
                tx.send(raw)  # client already set client_id + expected_count
                return
            elif t == data_type:
                tx.send(raw)  # forward raw; client_id already stamped by client
            else:
                logging.error(f"client handler got unexpected msg type {t}")
                return
