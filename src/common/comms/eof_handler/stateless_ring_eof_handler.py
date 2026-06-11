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


class StatelessRingEOFHandler(RingEOFHandler, StatelessEOFHandler):
    def __init__(self, id2: int, mom_ring: MOMRing, external_txs: Iterable[MOM]):
        self.id = id2
        self.mom_ring = mom_ring
        self.external_txs = [tx.clone() for tx in external_txs]
        self.processed_counts: dict[UUID, int] = {}
        self.sent_data: dict[UUID, int] = {}
        self.thread_handle: Thread | None = None
        self.mtx = Lock()

    def handle(self, eof: EOF):
        with self.mtx:
            eof.processed_count = self.processed_counts.get(eof.client_id, 0)
            eof.next_expected_count = self.sent_data.get(eof.client_id, 0)
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
                eof.expected_count = eof.next_expected_count
                logging.info(f"downstreaming eof: {eof.__dict__}")
                for tx in self.external_txs:
                    tx.send(eof.serialize())
                maybe_crash("after_downstream_eof_before_ack")
            else:
                with self.mtx:
                    eof.processed_count = self.processed_counts.get(eof.client_id, 0)
                    eof.next_expected_count = self.sent_data.get(eof.client_id, 0)
                time.sleep(EOF_LOOP_SECS / self.mom_ring.nnodes())
                logging.info(f"restarting eof ring round: {eof.__dict__}")
                mom_ring_tx.send(eof.serialize())
        else:
            with self.mtx:
                eof.processed_count += self.processed_counts.get(eof.client_id, 0)
                eof.next_expected_count += self.sent_data.get(eof.client_id, 0)
            logging.info(f"forwarding internal eof: {eof.__dict__}")
            mom_ring_tx.send(eof.serialize())

        ack()
