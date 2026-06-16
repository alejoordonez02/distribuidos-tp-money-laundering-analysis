import logging
from typing import Any, Callable, Optional, Sequence
from uuid import UUID

from merge_fns import MergeFn

from common.checkpoint import Checkpointer, dispatch
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.ring_node import RingNode
from common.comms.messages import Message, deserialize_message
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer
from common.graceful_shutdown import setup_graceful_shutdown


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


class RingMerge(RingNode):
    """Broadcast-join merge over a ring of N peers, completing per-peer.

    The left input (a small global state) is broadcast so every peer holds it in full;
    the right input (the large stream) is sharded, so each peer joins only its shard.
    One thread consumes left, right and the barrier ring. A peer is locally complete
    when BOTH sides are fully received; that combined count is fed to RingCompletion.
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
        super().__init__(
            node_id, rc, consumer, ring, external_txs,
            ring_queue, ring_exchange, data_prefetch, checkpointer,
        )
        self.fn = fn
        self.counts = counts
        self._left_queue = left_queue
        self._left_exchange = left_exchange
        self._right_queue = right_queue
        self._right_exchange = right_exchange

    def start(self):
        setup_graceful_shutdown(self.stop)
        if self.checkpointer and self.checkpointer.restore():
            logging.info("restored state from checkpoint")
        self.consumer.add_queue(
            self._left_queue, self._on_left_msg, prefetch=self._data_prefetch,
            durable=True, exchange=self._left_exchange, routing_key=str(self.node_id),
        )
        self.consumer.add_queue(
            self._right_queue, self._on_right_msg, prefetch=self._data_prefetch,
            durable=True, exchange=self._right_exchange, routing_key=str(self.node_id),
        )
        self._add_ring_queue()
        self.consumer.start()
        self.stop()

    def _on_left_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg = deserialize_message(body)
        dispatch(self.checkpointer, msg, ack, self._on_left_eof, self._on_left_data)

    def _on_left_data(self, msg: Message):
        self.fn.left(msg)  # type: ignore[arg-type]
        self.rc.on_data(msg.client_id)

    def _on_left_eof(self, eof: Message):
        self.counts.left[eof.client_id] = eof.expected_count  # type: ignore[attr-defined]
        self._maybe_complete(eof.client_id)

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
        if client_id in self.counts.left and client_id in self.counts.right:
            combined = self.counts.left[client_id] + self.counts.right[client_id]
            self._run(self.rc.on_upstream_eof(client_id, combined))
        if self.checkpointer:
            self.checkpointer.flush(force=True)

    def _emit(self, client_id: UUID):
        sent: dict[int, int] = {}
        for i, merged in enumerate(self.fn.get_result(client_id)):
            shard = i % len(self.external_txs)
            self.external_txs[shard].send(merged.serialize())
            sent[shard] = sent.get(shard, 0) + 1
        self._run(self.rc.report_sent(client_id, sent))
