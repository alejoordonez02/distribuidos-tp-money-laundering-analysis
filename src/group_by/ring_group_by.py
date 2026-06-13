from typing import Optional, Sequence

from group_by_fns import GroupByFn

from common.checkpoint import Checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.ring_node import StatelessRingNode
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.messages import Message
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer


class RingGroupBy(StatelessRingNode):
    """Stateless group-by over an affinity input shard, completing per-peer. Fans each
    input out to its grouped partials, routing each to a downstream shard by affinity."""

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
        super().__init__(
            sent, node_id, rc, consumer, ring, external_txs,
            ring_queue, ring_exchange, data_prefetch, checkpointer,
            data_queue=data_queue, data_exchange=data_exchange,
        )
        self.fn = fn

    def _on_data(self, msg: Message):
        for group, affinity in self.fn.group_by(msg):
            shard = affinity % len(self.external_txs)
            self.external_txs[shard].send(group.serialize())
            self.sent.add(msg.client_id, shard)
        self.rc.on_data(msg.client_id)
