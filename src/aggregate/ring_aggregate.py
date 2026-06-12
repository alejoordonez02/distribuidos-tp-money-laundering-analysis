import logging
import time
from typing import Callable, Optional, Sequence

from aggregate_fns import AggregateFn

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


class RingAggregate:
    """Stateful aggregate over an affinity input shard, completing per-peer.

    One thread consumes both the data shard and the barrier ring, so every state
    mutation (business state + RingCompletion phase) rides one atomic checkpoint and
    a crash restores a consistent snapshot. Each peer gets its own upstream EOF and
    completes locally; a single barrier token then collects every peer's per-shard
    sent counts and the leader forwards one downstream EOF per shard.
    """

    def __init__(
        self,
        fn: AggregateFn,
        node_id: int,
        rc: RingCompletion,
        consumer: MultiQueueConsumer,
        ring: MOMRing,
        external_txs: Sequence[MOM],
        data_queue: str,
        data_exchange: str,
        ring_queue: str,
        ring_exchange: str,
        data_prefetch: int,
        checkpointer: Optional[Checkpointer] = None,
        broadcast_downstream: bool = False,
    ):
        self.fn = fn
        self.node_id = node_id
        self.rc = rc
        self.consumer = consumer
        self.ring = ring
        self.external_txs = external_txs
        self.checkpointer = checkpointer
        # broadcast: send every result to ALL downstream replicas (a small global
        # state fanned out to N broadcast-join merges) instead of sharding by affinity.
        self.broadcast_downstream = broadcast_downstream
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
        self.fn.aggregate(msg)
        self.rc.on_data(msg.client_id)

    def _on_eof(self, eof: Message):
        # this peer's input shard is locally complete -> emit + report, then persist
        # the phase before dispatch acks so a redelivered EOF does not re-emit.
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
                self._emit_results(action.client_id)
            elif isinstance(action, Forward):
                self._forward(action.token)
            elif isinstance(action, DownstreamEOF):
                self._downstream_eof(action)

    def _emit_results(self, client_id):
        sent: dict[int, int] = {}
        for aggregated, affinity in self.fn.get_result(client_id):
            if self.broadcast_downstream:
                # every replica gets the full state; count it per shard so the barrier
                # forwards each downstream peer an EOF for the whole (broadcast) total.
                for shard, tx in enumerate(self.external_txs):
                    tx.send(aggregated.serialize())
                    sent[shard] = sent.get(shard, 0) + 1
            else:
                shard = affinity % len(self.external_txs)
                self.external_txs[shard].send(aggregated.serialize())
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
