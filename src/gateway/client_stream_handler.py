import logging
from threading import Thread
from typing import Callable
from uuid import uuid4

from common.comms.connection import Connection
from common.comms.messages import (
    PREFIX_RANGE,
    TYPE_RANGE,
    Hello,
    HelloAck,
    Message,
    MessageType,
    peek_type,
)
from common.comms.middleware import (
    MOMQueue,
    StampingMOM,
    UniqueStampingMOM,
    derive_producer_id,
)


class ClientStreamHandler:
    def __init__(
        self,
        conn: Connection,
        register: "Callable[[ClientStreamHandler], None]",
        trans_tx_factory: Callable[[], MOMQueue],
        accs_tx_factory: Callable[[], MOMQueue],
    ):
        self.id = uuid4()
        self.conn = conn
        self._register = register
        self.trans_tx_factory = trans_tx_factory
        self.accs_tx_factory = accs_tx_factory
        self.handle: Thread

    def start(self):
        """Start a thread forwarding this stream's transactions and accounts."""
        self.handle = Thread(target=self._run)
        self.handle.start()

    def send(self, msg: Message):
        self.conn.send(msg.serialize())

    def _run(self):
        Hello.deserialize(self.conn.recv())
        self.id = uuid4()
        self._register(self)
        self.conn.send(HelloAck(self.id).serialize())

        # unique producer per message so the competing default_filter's watermark
        # dedup stays exact under out-of-order/crash redelivery
        trans_tx = UniqueStampingMOM(
            self.trans_tx_factory(), derive_producer_id(str(self.id), 0, 0)
        )
        accs_tx = StampingMOM(
            self.accs_tx_factory(), derive_producer_id(str(self.id), 0, 1)
        )
        self._forward_until_eof(trans_tx, MessageType.TRANSACTIONS)
        self._forward_until_eof(accs_tx, MessageType.ACCOUNTS)
        logging.info("finished forwarding all client's data")

    def _forward_until_eof(self, tx: MOMQueue, data_type: MessageType):
        while True:
            raw = self.conn.recv()
            if not raw:
                logging.error("client connection closed before EOF")
                return
            t = peek_type(raw)
            if t == MessageType.EOF:
                tx.send(self._stamp_id(raw))
                return
            elif t == data_type:
                tx.send(self._stamp_id(raw))
            else:
                logging.error(f"client handler got unexpected msg type {t}")
                return

    def _stamp_id(self, raw: bytes) -> bytes:
        """Overwrite the 16-byte client_id prefix with the gateway-minted id,
        preserving everything after it (producer_id, seq, payload)."""
        return raw[TYPE_RANGE] + self.id.bytes + raw[PREFIX_RANGE.stop :]
