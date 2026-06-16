from typing import Optional, Sequence

from filter_fns import FilterFn

from common.checkpoint import Checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.ring_node import StatelessRingNode, shard_of
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer


class RingBroadcastFilter(StatelessRingNode):
    """The default filter as an affinity ring: one input shard per peer, broadcast to
    every UC route (and sharded to the period-A/B routes), completing per-peer."""

    def __init__(
        self,
        broadcast_routes: Sequence[tuple[MOM, FilterFn]],
        sharded_routes: Sequence[tuple[Sequence[MOM], FilterFn]],
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
        external_txs = [tx for tx, _ in broadcast_routes] + [
            tx for shards, _ in sharded_routes for tx in shards
        ]
        super().__init__(
            sent, node_id, rc, consumer, ring, external_txs,
            ring_queue, ring_exchange, data_prefetch, checkpointer,
            data_queue=data_queue, data_exchange=data_exchange,
        )
        self.broadcast_routes = broadcast_routes
        self.sharded_routes = sharded_routes

    def _on_data(self, msg):
        shard = 0
        for tx, filter_fn in self.broadcast_routes:
            tx.send(filter_fn.filter(msg).serialize())
            self.sent.add(msg.client_id, shard)
            shard += 1
        for shards, filter_fn in self.sharded_routes:
            i = shard_of(msg, len(shards))
            shards[i].send(filter_fn.filter(msg).serialize())
            self.sent.add(msg.client_id, shard + i)
            shard += len(shards)
        self.rc.on_data(msg.client_id)
