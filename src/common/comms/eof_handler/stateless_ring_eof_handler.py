import logging
import time
from threading import Lock, Thread
from typing import Callable, Iterable
from uuid import UUID

from common.comms.messages import EOF
from common.comms.middleware import MOM, MOMRing
from common.fault_injection import maybe_crash

from .eof_handler import StatelessEOFHandler
from .ring_eof_handler import RingEOFHandler

EOF_LOOP_SECS = 1


def _merge_shards(into: dict[int, int], frm: dict[int, int]):
    for shard, count in frm.items():
        into[shard] = into.get(shard, 0) + count


class StatelessRingEOFHandler(RingEOFHandler, StatelessEOFHandler):
    def __init__(self, id2: int, mom_ring: MOMRing, external_txs: Iterable[MOM]):
        self.id = id2
        self.mom_ring = mom_ring
        # all downstream shards: the ring sums sent per shard and the leader sends
        # each downstream peer its own per-shard expected_count.
        self.external_txs = [tx.clone() for tx in external_txs]
        self.processed_counts: dict[UUID, int] = {}
        self.sent_data: dict[UUID, dict[int, int]] = {}
        self.thread_handle: Thread | None = None
        self.mtx = Lock()

    def handle(self, eof: EOF):
        with self.mtx:
            eof.processed_count = self.processed_counts.get(eof.client_id, 0)
            eof.next_expected_per_shard = dict(self.sent_data.get(eof.client_id, {}))
        eof.origin = self.id
        logging.info(f"starting eof ring round, {eof.__dict__}")
        self.mom_ring.send(eof.serialize())

    def _handle_ring_message(
        self, bytes2: bytes, ack: Callable, nack: Callable, mom_ring_tx: MOMRing
    ):
        eof: EOF = EOF.deserialize(bytes2)  # type: ignore[reportAssignmentType]

        # counts are never zeroed (monotonic, checkpointed -> crash-safe); the leader
        # reseeds the lap accumulator so they are not double-added across laps
        if eof.origin == self.id:
            if eof.processed_count >= eof.expected_count:
                self._downstream_per_shard(eof)
                maybe_crash("after_downstream_eof_before_ack")
            else:
                with self.mtx:
                    eof.processed_count = self.processed_counts.get(eof.client_id, 0)
                    eof.next_expected_per_shard = dict(
                        self.sent_data.get(eof.client_id, {})
                    )
                time.sleep(EOF_LOOP_SECS / self.mom_ring.nnodes())
                logging.info(f"restarting eof ring round: {eof.__dict__}")
                mom_ring_tx.send(eof.serialize())
        else:
            with self.mtx:
                eof.processed_count += self.processed_counts.get(eof.client_id, 0)
                mine = dict(self.sent_data.get(eof.client_id, {}))
            _merge_shards(eof.next_expected_per_shard, mine)
            logging.info(f"forwarding internal eof: {eof.__dict__}")
            mom_ring_tx.send(eof.serialize())

        ack()

    def _downstream_per_shard(self, eof: EOF):
        # one EOF per downstream shard, each carrying that shard's total across the
        # cluster as its expected_count (a single downstream is just shard 0).
        for shard, tx in enumerate(self.external_txs):
            shard_eof = EOF(
                eof.client_id,
                expected_count=eof.next_expected_per_shard.get(shard, 0),
            )
            logging.info(f"downstreaming per-shard eof: {shard_eof.__dict__}")
            tx.send(shard_eof.serialize())
