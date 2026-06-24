from typing import Optional, Sequence

from group_by_fns import GroupByFn

from common.checkpoint import Checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.eof_handler.ring_node import StatelessRingNode
from common.comms.messages import Message
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer
from common.comms.middleware.stamping_mom import sub_producer


class RingGroupBy(StatelessRingNode):
    """Stateless group-by over an affinity input shard, completing per-peer. Builds each
    input's grouped partials once and fans them out to one or more downstream fleets,
    routing to a shard within each fleet by affinity. A single fleet is the common case
    (one downstream aggregate); several fleets feed distinct aggregates from one build
    (UC4's full-graph and degree branches). The flat external_txs (every fleet's shards
    concatenated) drives per-shard EOF unchanged."""

    def __init__(
        self,
        fn: GroupByFn,
        node_id: int,
        rc: RingCompletion,
        sent: SentCounts,
        consumer: MultiQueueConsumer,
        ring: MOMRing,
        fleets: Sequence[Sequence[MOM]],
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
        for sub_index, (group, affinity) in enumerate(self.fn.group_by(msg)):
            payload = group.serialize()
            producer = sub_producer(msg.producer_id, sub_index)
            base = 0
            for fleet in self.fleets:
                shard = affinity % len(fleet)
                fleet[shard].send_stamped(payload, producer, msg.seq)  # type: ignore[attr-defined]
                self.sent.add(msg.client_id, base + shard)
                base += len(fleet)
        self._run(self.rc.on_data(msg.client_id))
