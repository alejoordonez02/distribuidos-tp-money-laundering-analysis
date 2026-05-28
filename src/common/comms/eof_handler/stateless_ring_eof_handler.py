import logging
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from common.comms.messages import EOF
from common.comms.middleware import MOMQueue, MOMRing

from .eof_handler import StatelessEOFHandler
from .ring_eof_handler import RingEOFHandler


class StatelessRingEOFHandler(RingEOFHandler, StatelessEOFHandler):
    def __init__(self, mom_ring: MOMRing, txs: list[MOMQueue]):
        self.mom_ring = mom_ring
        self.txs = [tx.clone() for tx in txs]
        self.processed_counts: dict[UUID, int] = {}
        self.thread_handle: Thread
        self.mtx = Lock()

    def handle(self, eof: EOF):
        eof.processed_count = 0
        self.mom_ring.send(eof.serialize())

    def _handle_ring_message(
        self, bytes2: bytes, ack: Callable, nack: Callable, mom_ring_tx: MOMRing
    ):
        eof: EOF = EOF.deserialize(bytes2)  # type: ignore[reportAssignmentType]

        with self.mtx:
            eof.processed_count += self.processed_counts.get(eof.client_id, 0)
            self.processed_counts[eof.client_id] = 0

            if eof.processed_count == eof.expected_count:
                logging.info(f"downstreaming eof: {eof.__dict__}")
                for tx in self.txs:
                    tx.send(eof.serialize())

            else:
                logging.info(f"sending internal eof: {eof.__dict__}")
                mom_ring_tx.send(eof.serialize())

            ack()
