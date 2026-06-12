import logging
import time
from typing import Callable, Optional, Sequence

from group_by_fns import GroupByFn

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
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer
from common.graceful_shutdown import setup_graceful_shutdown

RING_THROTTLE_SECS = 1


class RingGroupBy:
    """Stateless group-by over an affinity input shard, completing per-peer.

    The affinity counterpart of the competing GroupBy: one thread consumes the data
    shard and the barrier ring, fanning each input out to its grouped partials and
    routing each by affinity to a downstream shard (no state to emit at EOF). It
    tracks how many it sent per downstream shard; when its input shard is locally
    complete, a single barrier token collects every peer's per-shard sent counts and
    the leader forwards one downstream EOF per shard. Mirrors RingFilter; the only
    difference is the per-input fan-out instead of a single filtered send.
    """

    def __init__(
        self,
        fn: GroupByFn,
        node_id: int,
        rc: RingCompletion,
        sent: SentCounts,
        consumer: MultiQueueConsumer,
        ring: MOMRing,
        external_txs: Sequence[MOM],
        data_queue: str,
        data_exchange: str,
        ring_queue: str,
        ring_exchange: str,
        data_prefetch: int,
        checkpointer: Optional[Checkpointer] = None,
    ):
        self.fn = fn
        self.node_id = node_id
        self.rc = rc
        self.sent = sent
        self.consumer = consumer
        self.ring = ring
        self.external_txs = external_txs
        self.checkpointer = checkpointer
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

    def _on_data_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg = deserialize_message(body)
        dispatch(self.checkpointer, msg, ack, self._on_eof, self._on_data)

    def _on_data(self, msg: Message):
        # fan out: each input yields one or more grouped partials, each routed to a
        # downstream shard by affinity. Deterministic so a re-emit after a crash lands
        # on the same shard with the same restored seq (single downstream is shard 0).
        for group, affinity in self.fn.group_by(msg):
            shard = affinity % len(self.external_txs)
            self.external_txs[shard].send(group.serialize())
            self.sent.add(msg.client_id, shard)
        self.rc.on_data(msg.client_id)

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
                # stateless: nothing to emit at EOF, just report what we already sent
                self._run(self.rc.report_sent(action.client_id, self.sent.pop(action.client_id)))
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
