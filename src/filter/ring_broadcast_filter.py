from typing import Callable, Optional, Sequence

from filter_fns import FilterFn

from common.checkpoint import Checkpointer, dispatch
from common.comms.messages import deserialize_message
from common.comms.middleware import MOM, InputContext, MOMRing, MultiQueueConsumer

from common.comms.eof_handler.ring_completion import RingCompletion
from ring_filter import RingFilter, SentCounts


class RingBroadcastFilter(RingFilter):
    """The default filter as an affinity ring: one input shard per peer, broadcast
    to every UC route, completing per-peer.

    The competing default filter blocks its single consume thread on synchronous
    fan-out and never resumes its EOF ring-round after a crash. This is the affinity
    counterpart: the gateway routes each transaction to one peer's durable shard
    queue, so a peer consumes only its slice on one thread (data + ring), broadcasts
    each message to the six UC routes (and the sharded period-B route), and reports
    its per-route sent counts to the barrier. The leader then forwards one downstream
    EOF per route with the cluster-wide total — the same per-shard EOF the competing
    StatelessRingEOFHandler emitted, so the (still competing) downstream is unchanged.

    Output stamping stays DerivedStampingMOM (id derived from the input) because the
    downstream is still competing: a re-emit after a crash lands on the same shard
    (durable queue) and derives the same id, so it dedups downstream.
    """

    def __init__(
        self,
        broadcast_routes: Sequence[tuple[MOM, FilterFn]],
        sharded_routes: Sequence[tuple[Sequence[MOM], FilterFn]],
        input_ctx: InputContext,
        node_id: int,
        rc: RingCompletion,
        sent: SentCounts,
        consumer: MultiQueueConsumer,
        ring: MOMRing,
        data_queue: str,
        data_exchange: str,
        ring_queue: str,
        ring_exchange: str,
        data_prefetch: int,
        checkpointer: Optional[Checkpointer] = None,
    ):
        # shard order: the broadcast routes first (one shard each), then each sharded
        # route's shards — so report_sent / _downstream_eof index them consistently.
        external_txs = [tx for tx, _ in broadcast_routes] + [
            tx for shards, _ in sharded_routes for tx in shards
        ]
        super().__init__(
            fn=None,  # type: ignore[arg-type]
            node_id=node_id,
            rc=rc,
            sent=sent,
            consumer=consumer,
            ring=ring,
            external_txs=external_txs,
            data_queue=data_queue,
            data_exchange=data_exchange,
            ring_queue=ring_queue,
            ring_exchange=ring_exchange,
            data_prefetch=data_prefetch,
            checkpointer=checkpointer,
        )
        self.broadcast_routes = broadcast_routes
        self.sharded_routes = sharded_routes
        self.input_ctx = input_ctx

    def _on_data_msg(self, body: bytes, ack: Callable, _nack: Callable):
        msg = deserialize_message(body)
        # publish the input identity before the fan-out so DerivedStampingMOM stamps
        # each output with an id derived from it (whichever peer's shard processed it).
        dispatch(
            self.checkpointer, msg, ack, self._on_eof, self._on_data, self.input_ctx
        )

    def _on_data(self, msg):
        shard = 0
        # broadcast routes: every input reaches every route, each its own shard so the
        # ring forwards each route its own EOF.
        for tx, filter_fn in self.broadcast_routes:
            tx.send(filter_fn.filter(msg).serialize())
            self.sent.add(msg.client_id, shard)
            shard += 1

        # sharded routes: partition output across N shards, deterministically by
        # message identity so a re-emit after a crash lands on the same shard.
        for shards, filter_fn in self.sharded_routes:
            i = (int.from_bytes(msg.producer_id[-4:], "big") + msg.seq) % len(shards)
            shards[i].send(filter_fn.filter(msg).serialize())
            self.sent.add(msg.client_id, shard + i)
            shard += len(shards)

        self.rc.on_data(msg.client_id)
