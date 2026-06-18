from typing import Optional, Sequence

from aggregate_fns import AggregateFn

from common.checkpoint import Checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.ring_node import RingNode
from common.comms.messages import Message
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer


class RingAggregate(RingNode):
    """Stateful aggregate over an affinity input shard, completing per-peer. Emits its
    accumulated results only once the input shard is locally complete, sharding each by
    affinity (or broadcasting the whole small state to every downstream peer)."""

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
        super().__init__(
            node_id, rc, consumer, ring, external_txs,
            ring_queue, ring_exchange, data_prefetch, checkpointer,
            data_queue=data_queue, data_exchange=data_exchange,
        )
        self.fn = fn
        self.broadcast_downstream = broadcast_downstream

    def _on_data(self, msg: Message):
        self.fn.aggregate(msg)
        self.rc.on_data(msg.client_id)

    def _discard(self, client_id):
        self.fn.discard(client_id)

    def _emit(self, client_id):
        sent: dict[int, int] = {}
        for aggregated, affinity in self.fn.get_result(client_id):
            if self.broadcast_downstream:
                # broadcast: every peer gets the full state, counted per shard so the
                # barrier forwards each downstream peer an EOF for the whole total.
                for shard, tx in enumerate(self.external_txs):
                    tx.send(aggregated.serialize())
                    sent[shard] = sent.get(shard, 0) + 1
            else:
                shard = affinity % len(self.external_txs)
                self.external_txs[shard].send(aggregated.serialize())
                sent[shard] = sent.get(shard, 0) + 1
        self._run(self.rc.report_sent(client_id, sent))
