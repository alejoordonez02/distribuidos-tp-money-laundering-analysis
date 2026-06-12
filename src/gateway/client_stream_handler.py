import logging
from threading import Thread
from typing import Callable, Sequence
from uuid import uuid4

from common.comms.connection import Connection
from common.comms.messages import (
    PREFIX_RANGE,
    TYPE_RANGE,
    EOF,
    Hello,
    HelloAck,
    Message,
    MessageType,
    peek_type,
)
from common.comms.middleware import (
    MOM,
    SeqCounter,
    StampingMOM,
    derive_producer_id,
)


class ClientStreamHandler:
    def __init__(
        self,
        conn: Connection,
        register: "Callable[[ClientStreamHandler], None]",
        trans_tx_factory: Callable[[], Sequence[MOM]],
        accs_tx_factory: Callable[[], Sequence[MOM]],
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

        # Each stream is round-robined across its downstream ring's shards. A single
        # producer per stream with a monotonic seq dedups exactly under the affinity
        # routing (each shard is consumed in order by one peer); one shared counter
        # across the shard publishers keeps the seq monotonic, each shard durable.
        trans_counter = SeqCounter()
        trans_base = derive_producer_id(str(self.id), 0, 0)
        trans_shards = [
            StampingMOM(tx, trans_base, trans_counter)
            for tx in self.trans_tx_factory()
        ]
        accs_counter = SeqCounter()
        accs_base = derive_producer_id(str(self.id), 0, 1)
        accs_shards = [
            StampingMOM(tx, accs_base, accs_counter) for tx in self.accs_tx_factory()
        ]
        self._forward_sharded(trans_shards, MessageType.TRANSACTIONS)
        self._forward_sharded(accs_shards, MessageType.ACCOUNTS)
        logging.info("finished forwarding all client's data")

    def _forward_sharded(self, shard_txs: Sequence[MOM], data_type: MessageType):
        """Route each message to one shard by round-robin (even split, stable once
        landed since shard queues are durable), counting per shard. On EOF send each
        shard its own EOF carrying that shard's expected_count, so each downstream peer
        knows when its input slice is complete. A single working queue is just N=1."""
        n = len(shard_txs)
        counts = [0] * n
        rr = 0
        while True:
            raw = self.conn.recv()
            if not raw:
                logging.error("client connection closed before EOF")
                return
            t = peek_type(raw)
            if t == MessageType.EOF:
                for i, tx in enumerate(shard_txs):
                    tx.send(EOF(self.id, expected_count=counts[i]).serialize())
                return
            elif t == data_type:
                shard_txs[rr % n].send(self._stamp_id(raw))
                counts[rr % n] += 1
                rr += 1
            else:
                logging.error(f"client handler got unexpected msg type {t}")
                return

    def _stamp_id(self, raw: bytes) -> bytes:
        """Overwrite the 16-byte client_id prefix with the gateway-minted id,
        preserving everything after it (producer_id, seq, payload)."""
        return raw[TYPE_RANGE] + self.id.bytes + raw[PREFIX_RANGE.stop :]
