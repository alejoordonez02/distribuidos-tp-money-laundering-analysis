from filter_fns import FilterFn

from common.checkpoint import Checkpointer
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.ring_node import StatelessRingNode, shard_of
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.messages import Message
from common.comms.middleware import MOM, MOMRing, MultiQueueConsumer
from typing import Optional, Sequence


class RingFilter(StatelessRingNode):
    """Stateless filter over an affinity input shard, completing per-peer. Forwards
    each filtered message to one downstream shard chosen by message identity."""

    def __init__(
        self,
        fn: FilterFn,
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
        shard = shard_of(msg, len(self.external_txs))
        self.external_txs[shard].send_stamped(  # type: ignore[attr-defined]
            self.fn.filter(msg).serialize(), msg.producer_id, msg.seq
        )
        self.sent.add(msg.client_id, shard)
        self._run(self.rc.on_data(msg.client_id))
