from __future__ import annotations

import logging
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
    """Owns one client connection end to end.

    A reader thread forwards the client's uploaded data into the pipeline. A response
    thread consumes this client's *own* broker queue (bound by `client_id` to the
    responses exchange) and writes each response straight to the client's socket,
    acking only once the bytes are on the wire.

    Because each client has a dedicated queue, consumer thread and broker connection,
    a slow or dead client only ever backs up (or tears down) its own path: there is no
    shared response router that another client could stall.
    """

    def __init__(
        self,
        conn: Connection,
        register: Callable[[ClientStreamHandler], None],
        unregister: Callable[[UUID], None],
        responses_rx_factory: Callable[[UUID], MOM],
        trans_tx_factory: Callable[[], Sequence[MOM]],
        accs_tx_factory: Callable[[], Sequence[MOM]],
    ):
        self.id = uuid4()
        self.conn = conn
        self._register = register
        self._unregister = unregister
        self._responses_rx_factory = responses_rx_factory
        self.trans_tx_factory = trans_tx_factory
        self.accs_tx_factory = accs_tx_factory
        self.handle: Thread
        self._responses_rx: MOM | None = None
        self._response_thread: Thread | None = None
        self._close_lock = Lock()
        self._closed = False

    def start(self):
        """Start the reader thread; it brings up the response consumer once the
        handshake is done (so no broker resources are opened for a client that drops
        before saying Hello)."""
        self.handle = Thread(target=self._run)
        self.handle.start()

    def stop(self):
        """Stop this client's session: close the socket, stop its response consumer and
        wait for the response thread to finish, keeping the inner thread transparent to
        the caller. Idempotent; used on gateway shutdown. Closing the socket also unblocks
        a response thread stuck mid-send to a slow client."""
        self._close()
        if self._responses_rx is not None:
            self._responses_rx.stop_consuming()
        if self._response_thread is not None:
            self._response_thread.join()

    def _run(self):
        try:
            raw = self.conn.recv()
            if not raw:
                logging.warning("client disconnected before handshake")
                self._close()
                return
            Hello.deserialize(raw)
            self._register(self)
            self.conn.send(HelloAck(self.id).serialize())
            self._start_response_consumer()
        except OSError:
            # The client dropped during the handshake: Connection surfaces it as an
            # OSError (it never swallows an unexpected error), so handle it here instead
            # of letting the reader thread die. Teardown is idempotent and covers the
            # case where the client had already registered.
            logging.warning("client disconnected during handshake")
            self._teardown()
            return

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

    def _start_response_consumer(self):
        """Bind this client's own response queue and start draining it to the socket.
        The queue exists from here on — well before any response is produced (the
        pipeline only emits once the client's full upload has been processed)."""
        self._responses_rx = self._responses_rx_factory(self.id)
        self._response_thread = Thread(target=self._consume_responses)
        self._response_thread.start()

    def _consume_responses(self):
        """Drain this client's response queue to its socket until the queue is stopped
        or the client becomes unreachable. Owns the broker connection's lifecycle:
        closes it on exit, which auto-deletes the exclusive per-client queue."""
        try:
            self._responses_rx.start_consuming(self._deliver_response)  # type: ignore[union-attr]
        finally:
            self._responses_rx.close()  # type: ignore[union-attr]

    def _deliver_response(self, raw: bytes, ack: Callable, nack: Callable):
        """Write one response to the client and ack only after it is on the wire. If
        the send fails the message stays un-acked and the client is torn down; nothing
        is ever acked (and thus dropped) without having reached the socket."""
        try:
            self.conn.send(raw)
        except OSError as e:
            logging.warning("client %s unreachable; tearing down (%s)", self.id, e)
            self._teardown()
            return
        ack()

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
        """Unregister the client, stop its response consumer and close its socket once.
        Idempotent, so the reader (abort) and response (failed send) paths can both call
        it safely."""
        self._unregister(self.id)
        if self._responses_rx is not None:
            self._responses_rx.stop_consuming()
        self._close()

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
