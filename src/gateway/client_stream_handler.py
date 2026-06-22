import logging
import os
from queue import Full, Queue
from threading import Lock, Thread
from typing import Callable, Sequence
from uuid import UUID, uuid4

from common.comms.transport import Connection
from common.comms.messages import (
    PREFIX_RANGE,
    TYPE_RANGE,
    Abort,
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

# Bound each client's in-memory outbound buffer. Responses are ~5 per client, so this
# is never reached in practice; it just keeps a slow client from growing unbounded.
OUTBOX_MAXSIZE = int(os.getenv("GATEWAY_CLIENT_OUTBOX_MAX", "1024"))


class ClientStreamHandler:
    """Owns one client connection. A reader thread forwards the client's uploaded data
    to the pipeline; a writer thread drains an in-memory outbox to the client's socket.
    The shared response router only enqueues (never touches the socket), so a slow or
    dead client blocks only its own writer, never the delivery to other clients."""

    def __init__(
        self,
        conn: Connection,
        register: "Callable[[ClientStreamHandler], None]",
        unregister: "Callable[[UUID], None]",
        trans_tx_factory: Callable[[], Sequence[MOM]],
        accs_tx_factory: Callable[[], Sequence[MOM]],
    ):
        self.id = uuid4()
        self.conn = conn
        self._register = register
        self._unregister = unregister
        self.trans_tx_factory = trans_tx_factory
        self.accs_tx_factory = accs_tx_factory
        self.handle: Thread
        self._writer: "Thread | None" = None
        self._outbox: "Queue[Message | None]" = Queue(maxsize=OUTBOX_MAXSIZE)
        self._close_lock = Lock()
        self._closed = False

    def start(self):
        """Start the reader thread; it spawns the writer once the handshake is done."""
        self.handle = Thread(target=self._run)
        self.handle.start()

    def send(self, msg: Message):
        """Enqueue a response for this client's writer thread. Never blocks the caller
        (the shared response router): if the outbox is full the response is dropped."""
        try:
            self._outbox.put_nowait(msg)
        except Full:
            logging.warning("client %s outbox full; dropping response", self.id)

    def stop(self):
        """Close the socket and signal the writer to exit (idempotent, non-blocking).
        Used on gateway shutdown; a blocked send is unblocked by closing the socket."""
        self._close()
        self._signal_writer_stop()

    def join(self):
        """Wait for the writer thread to finish (after `stop`)."""
        if self._writer is not None:
            self._writer.join()

    def _run(self):
        Hello.deserialize(self.conn.recv())
        self.id = uuid4()
        self._register(self)
        self.conn.send(HelloAck(self.id).serialize())
        self._writer = Thread(target=self._drain_outbox)
        self._writer.start()

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
        try:
            done = self._forward_sharded(trans_shards, MessageType.TRANSACTIONS) and \
                self._forward_sharded(accs_shards, MessageType.ACCOUNTS)
        except OSError:
            if not self._closed:
                logging.exception("client %s reader failed", self.id)
            return
        if not done:
            self._abort([*trans_shards, *accs_shards])
            return
        logging.info("finished forwarding all client's data")

    def _drain_outbox(self):
        """Drain queued responses to this client's socket. On send failure the client
        is treated as gone: stop routing to it and close its socket once."""
        while True:
            msg = self._outbox.get()
            if msg is None:
                return
            try:
                self.conn.send(msg.serialize())
            except OSError as e:
                logging.warning(
                    "client %s unreachable; dropping responses (%s)", self.id, e
                )
                self._teardown()
                return

    def _forward_sharded(self, shard_txs: Sequence[MOM], data_type: MessageType) -> bool:
        """Route each message to one shard by round-robin (even split, stable once
        landed since shard queues are durable), counting per shard. On EOF send each
        shard its own EOF carrying that shard's expected_count, so each downstream peer
        knows when its input slice is complete. A single working queue is just N=1.

        Returns True once the stream's EOF was forwarded, False if the client dropped
        before sending it (so the caller can purge the partial data)."""
        n = len(shard_txs)
        counts = [0] * n
        rr = 0
        while True:
            raw = self.conn.recv()
            if not raw:
                logging.error("client connection closed before EOF")
                return False
            t = peek_type(raw)
            if t == MessageType.EOF:
                for i, tx in enumerate(shard_txs):
                    tx.send(EOF(self.id, expected_count=counts[i]).serialize())
                return True
            elif t == data_type:
                shard_txs[rr % n].send(self._stamp_id(raw))
                counts[rr % n] += 1
                rr += 1
            else:
                logging.error("client handler got unexpected msg type %s", t)
                return False

    def _abort(self, shards: Sequence[MOM]):
        """Tell the pipeline to drop this crashed client's partial data."""
        logging.warning("client %s dropped before EOF; aborting its data", self.id)
        for tx in shards:
            tx.send(Abort(self.id).serialize())
        self._teardown()

    def _teardown(self):
        """Unregister the client, stop its writer and close its socket once. Idempotent,
        so the reader (abort) and writer (failed send) paths can both call it safely."""
        self._unregister(self.id)
        self._signal_writer_stop()
        self._close()

    def _signal_writer_stop(self):
        try:
            self._outbox.put_nowait(None)
        except Full:
            # Outbox full means the writer is busy sending; closing the socket makes
            # that send fail and the writer exit, so the sentinel isn't needed.
            pass

    def _close(self):
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        try:
            self.conn.close()
        except OSError:
            pass

    def _stamp_id(self, raw: bytes) -> bytes:
        """Overwrite the 16-byte client_id prefix with the gateway-minted id,
        preserving everything after it (producer_id, seq, payload)."""
        return raw[TYPE_RANGE] + self.id.bytes + raw[PREFIX_RANGE.stop :]
