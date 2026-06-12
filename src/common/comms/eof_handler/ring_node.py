import logging
import time
from typing import Callable, Optional, Sequence

from common.checkpoint import Checkpointer, dispatch
from common.comms.eof_handler.ring_completion import (
    BarrierToken,
    DownstreamEOF,
    Emit,
    Forward,
    RingCompletion,
)
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.messages import EOF, Message, RingBarrier, deserialize_message
from common.comms.middleware import MOM, InputContext, MOMRing, MultiQueueConsumer
from common.graceful_shutdown import setup_graceful_shutdown

RING_THROTTLE_SECS = 1


def shard_of(msg: Message, nshards: int) -> int:
    """Pick a downstream shard from the message identity, so a re-emit after a crash
    lands on the same shard (a single downstream is just shard 0)."""
    return (int.from_bytes(msg.producer_id[-4:], "big") + msg.seq) % nshards


class RingNode:
    """Shared machinery for an affinity-ring controller.

    One thread consumes a data shard and the barrier ring, so every state mutation
    (business state + RingCompletion phase) rides one atomic checkpoint and a crash
    restores a consistent shard + barrier phase. When the input shard is locally
    complete a single barrier token collects every peer's per-shard sent counts and
    the leader forwards one downstream EOF per shard.

    Subclasses implement `_on_data` (process one input) and `_emit` (what to send when
    the local input is complete). A multi-input node (the merge) overrides `start` to
    register its own queues and reuses the ring/forward machinery below.
    """

    def __init__(
        self,
        node_id: int,
        rc: RingCompletion,
        consumer: MultiQueueConsumer,
        ring: MOMRing,
        external_txs: Sequence[MOM],
        ring_queue: str,
        ring_exchange: str,
        data_prefetch: int,
        checkpointer: Optional[Checkpointer] = None,
        input_ctx: Optional[InputContext] = None,
        data_queue: Optional[str] = None,
        data_exchange: Optional[str] = None,
    ):
        self.node_id = node_id
        self.rc = rc
        self.consumer = consumer
        self.ring = ring
        self.external_txs = external_txs
        self.checkpointer = checkpointer
        self.input_ctx = input_ctx
        self._data_queue = data_queue
        self._data_exchange = data_exchange
        self._ring_queue = ring_queue
        self._ring_exchange = ring_exchange
        self._data_prefetch = data_prefetch

    def start(self):
        setup_graceful_shutdown(self.stop)
        if self.checkpointer and self.checkpointer.restore():
            logging.info("restored state from checkpoint")
        self.consumer.add_queue(
            self._data_queue,
            self._on_data_msg,
            prefetch=self._data_prefetch,
            durable=True,
            exchange=self._data_exchange,
            routing_key=str(self.node_id),
        )
        self._add_ring_queue()
        self.consumer.start()
        self.stop()

    def _add_ring_queue(self):
        self.consumer.add_queue(
            self._ring_queue,
            self._on_ring_msg,
            prefetch=1,
            durable=True,
            exchange=self._ring_exchange,
            routing_key=str(self.node_id),
        )

    def stop(self):
        self.consumer.stop()
        self.consumer.close()
        self.ring.close()
        for tx in self.external_txs:
            tx.close()

    def _on_data_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg = deserialize_message(body)
        dispatch(self.checkpointer, msg, ack, self._on_eof, self._on_data, self.input_ctx)

    def _on_data(self, msg: Message):
        raise NotImplementedError

    def _emit(self, client_id):
        raise NotImplementedError

    def _on_eof(self, eof: Message):
        self._run(self.rc.on_upstream_eof(eof.client_id, eof.expected_count))  # type: ignore[attr-defined]
        if self.checkpointer:
            self.checkpointer.flush(force=True)

    def _on_ring_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg: RingBarrier = deserialize_message(body)  # type: ignore[assignment]
        self._run(self.rc.on_token(BarrierToken(msg.client_id, msg.origin, msg.sent_by)))
        if self.checkpointer:
            self.checkpointer.flush(force=True)
        ack()

    def _run(self, actions):
        for action in actions:
            if isinstance(action, Emit):
                self._emit(action.client_id)
            elif isinstance(action, Forward):
                self._forward(action.token)
            elif isinstance(action, DownstreamEOF):
                self._downstream_eof(action)

    def _forward(self, token: BarrierToken):
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


class StatelessRingNode(RingNode):
    """A stateless ring node (filter / group-by / converter): emits each input as it
    processes it, so at EOF it has nothing to emit and only reports the per-shard
    counts it already sent. Subclasses implement `_on_data`."""

    def __init__(self, sent: SentCounts, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sent = sent

    def _emit(self, client_id):
        self._run(self.rc.report_sent(client_id, self.sent.pop(client_id)))
