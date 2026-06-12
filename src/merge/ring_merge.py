import logging
import time
from typing import Any, Callable, Optional, Sequence
from uuid import UUID

from merge_fns import MergeFn

from common.checkpoint import Checkpointer, dispatch
from common.comms.eof_handler.ring_completion import (
    BarrierToken,
    DownstreamEOF,
    Emit,
    Forward,
    RingCompletion,
)
from common.comms.messages import EOF, Message, RingBarrier, deserialize_message
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer
from common.graceful_shutdown import setup_graceful_shutdown

RING_THROTTLE_SECS = 1


class MergeEofCounts:
    """Per-client left/right expected counts, snapshotable so they ride the checkpoint.

    The merge has two inputs; we only know the combined `expected` once BOTH side
    EOFs have arrived. These survive a crash that lands between the two EOFs so the
    combination is recomputed deterministically on restore.
    """

    def __init__(self):
        self.left: dict[UUID, int] = {}
        self.right: dict[UUID, int] = {}

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "left": {str(c): n for c, n in self.left.items()},
            "right": {str(c): n for c, n in self.right.items()},
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self.left = {UUID(c): n for c, n in snapshot.get("left", {}).items()}
        self.right = {UUID(c): n for c, n in snapshot.get("right", {}).items()}


class RingMerge:
    """Broadcast-join merge over a ring of N peers, completing per-peer.

    The left input (a small global state, e.g. UC3 averages) is broadcast so every
    peer holds it in full; the right input (the large stream) is sharded, so each
    peer spills and joins only its shard. One thread consumes left, right and the
    barrier ring, so every state mutation (business state + RingCompletion phase)
    rides one atomic checkpoint.

    A peer is locally complete when BOTH sides are fully received; we feed that to
    RingCompletion as a single combined count (left_expected + right_expected). A
    single barrier token then collects every peer's per-shard sent counts and the
    leader forwards one downstream EOF per shard — so the downstream sees one
    consolidated EOF, exactly as from a single merge.
    """

    def __init__(
        self,
        fn: MergeFn,
        node_id: int,
        rc: RingCompletion,
        counts: MergeEofCounts,
        consumer: MultiQueueConsumer,
        ring: MOMRing,
        external_txs: Sequence[MOM],
        left_queue: str,
        left_exchange: str,
        right_queue: str,
        right_exchange: str,
        ring_queue: str,
        ring_exchange: str,
        data_prefetch: int,
        checkpointer: Optional[Checkpointer] = None,
    ):
        self.fn = fn
        self.node_id = node_id
        self.rc = rc
        self.counts = counts
        self.consumer = consumer
        self.ring = ring
        self.external_txs = external_txs
        self.checkpointer = checkpointer
        self._left_queue = left_queue
        self._left_exchange = left_exchange
        self._right_queue = right_queue
        self._right_exchange = right_exchange
        self._ring_queue = ring_queue
        self._ring_exchange = ring_exchange
        self._data_prefetch = data_prefetch

    def start(self):
        setup_graceful_shutdown(self.stop)
        if self.checkpointer and self.checkpointer.restore():
            logging.info("restored state from checkpoint")
        self.consumer.add_queue(
            self._left_queue,
            self._on_left_msg,
            prefetch=self._data_prefetch,
            durable=True,
            exchange=self._left_exchange,
            routing_key=str(self.node_id),
        )
        self.consumer.add_queue(
            self._right_queue,
            self._on_right_msg,
            prefetch=self._data_prefetch,
            durable=True,
            exchange=self._right_exchange,
            routing_key=str(self.node_id),
        )
        self.consumer.add_queue(
            self._ring_queue,
            self._on_ring_msg,
            prefetch=1,
            durable=True,
            exchange=self._ring_exchange,
            routing_key=str(self.node_id),
        )
        self.consumer.start()
        self.stop()

    def stop(self):
        self.consumer.stop()
        self.consumer.close()
        self.ring.close()
        for tx in self.external_txs:
            tx.close()

    # --- left side (broadcast averages) ---
    def _on_left_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg = deserialize_message(body)
        dispatch(self.checkpointer, msg, ack, self._on_left_eof, self._on_left_data)

    def _on_left_data(self, msg: Message):
        self.fn.left(msg)  # type: ignore[arg-type]
        self.rc.on_data(msg.client_id)

    def _on_left_eof(self, eof: Message):
        self.counts.left[eof.client_id] = eof.expected_count  # type: ignore[attr-defined]
        self._maybe_complete(eof.client_id)

    # --- right side (sharded period-B stream) ---
    def _on_right_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg = deserialize_message(body)
        dispatch(self.checkpointer, msg, ack, self._on_right_eof, self._on_right_data)

    def _on_right_data(self, msg: Message):
        self.fn.right(msg)  # type: ignore[arg-type]
        self.rc.on_data(msg.client_id)

    def _on_right_eof(self, eof: Message):
        self.counts.right[eof.client_id] = eof.expected_count  # type: ignore[attr-defined]
        self._maybe_complete(eof.client_id)

    def _maybe_complete(self, client_id: UUID):
        # both side EOFs in -> the combined input is fully described; let
        # RingCompletion decide whether everything has been received and emit.
        # single consume thread (MultiQueueConsumer), so no lock is needed -- and
        # flush() takes the checkpointer lock, so it must not run under another.
        if client_id in self.counts.left and client_id in self.counts.right:
            combined = self.counts.left[client_id] + self.counts.right[client_id]
            self._run(self.rc.on_upstream_eof(client_id, combined))
        if self.checkpointer:
            self.checkpointer.flush(force=True)

    # --- barrier ring ---
    def _on_ring_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg: RingBarrier = deserialize_message(body)  # type: ignore[assignment]
        self._run(self.rc.on_token(BarrierToken(msg.client_id, msg.origin, msg.sent_by)))
        if self.checkpointer:
            self.checkpointer.flush(force=True)
        ack()

    def _run(self, actions):
        for action in actions:
            if isinstance(action, Emit):
                self._emit_results(action.client_id)
            elif isinstance(action, Forward):
                self._forward(action.token)
            elif isinstance(action, DownstreamEOF):
                self._downstream_eof(action)

    def _emit_results(self, client_id: UUID):
        sent: dict[int, int] = {}
        for i, merged in enumerate(self.fn.get_result(client_id)):
            shard = i % len(self.external_txs)
            self.external_txs[shard].send(merged.serialize())
            sent[shard] = sent.get(shard, 0) + 1
        self._run(self.rc.report_sent(client_id, sent))

    def _forward(self, token: BarrierToken):
        # throttle the leader's re-laps while it waits for stragglers to emit, so an
        # incomplete token does not spin the ring; non-leaders forward immediately.
        if token.origin == self.node_id and len(token.sent_by) < self.rc.n_nodes:
            time.sleep(RING_THROTTLE_SECS / self.rc.n_nodes)
        self.ring.send(
            RingBarrier(token.client_id, token.origin, token.sent_by).serialize()
        )

    def _downstream_eof(self, action: DownstreamEOF):
        for shard, tx in enumerate(self.external_txs):
            eof = EOF(
                action.client_id,
                expected_count=action.expected_per_shard.get(shard, 0),
            )
            logging.info(f"downstreaming per-shard eof: {eof.__dict__}")
            tx.send(eof.serialize())
