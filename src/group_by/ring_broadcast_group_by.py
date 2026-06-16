from typing import Optional, Sequence

from group_by_fns import GroupByFn

from common.checkpoint import Checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.ring_node import StatelessRingNode
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.messages import Message
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer


class RingBroadcastGroupBy(StatelessRingNode):
    """Group-by that builds its partials once and fans each one out to several downstream
    fleets, routing to a shard within each fleet by affinity. The flat external_txs (every
    fleet's shards concatenated) drives per-shard EOF exactly like RingBroadcastFilter, so
    a single builder can feed two distinct aggregates without recomputing the graph."""

    def __init__(
        self,
        fn: GroupByFn,
        fleets: Sequence[Sequence[MOM]],
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
        external_txs = [tx for fleet in fleets for tx in fleet]
        super().__init__(
            sent, node_id, rc, consumer, ring, external_txs,
            ring_queue, ring_exchange, data_prefetch, checkpointer,
            data_queue=data_queue, data_exchange=data_exchange,
        )
        self.fn = fn
        self.fleets = fleets

    def _on_data(self, msg: Message):
        for group, affinity in self.fn.group_by(msg):
            payload = group.serialize()
            base = 0
            for fleet in self.fleets:
                shard = affinity % len(fleet)
                fleet[shard].send(payload)
                self.sent.add(msg.client_id, base + shard)
                base += len(fleet)
        self.rc.on_data(msg.client_id)
