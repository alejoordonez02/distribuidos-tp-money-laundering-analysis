import logging
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from eof_handler import EOFHandler

from common.comms.messages import EOF
from common.comms.middleware import MOMQueue, MOMRing


class RingEOFHandler(EOFHandler):
    def __init__(self, mom_ring: MOMRing, txs: list[MOMQueue]):
        self.mom_ring = mom_ring
        self.txs = txs
        self.processed_counts: dict[UUID, int] = {}
        self.thread_handle: Thread
        self.mtx = Lock()

    def start(self):
        self.thread_handle = Thread(
            target=self.mom_ring.start_consuming, args=(self._handle_ring_eof,)
        )
        self.thread_handle.start()

    def stop_consuming(self):
        self.mom_ring.stop_consuming()

    def stop(self):
        self.stop_consuming()
        self.thread_handle.join()

    def close(self):
        self.mom_ring.close()

    def handle(self, eof: EOF):
        with self.mtx:
            eof.processed_count = 0
            self.mom_ring.send(eof.serialize())

    def add_processed_count(self, client_id: UUID):
        with self.mtx:
            if client_id not in self.processed_counts:
                self.processed_counts[client_id] = 0

            self.processed_counts[client_id] += 1

    def _handle_ring_eof(self, bytes2: bytes, ack: Callable, _: Callable):
        eof: EOF = EOF.deserialize(bytes2)  # type: ignore[reportAssignmentType]

        with self.mtx:
            # TODO: estoy lockeando porq no estoy manejando pika
            # thread-safetyness todavía
            eof.processed_count += self.processed_counts.get(eof.client_id, 0)
            self.processed_counts[eof.client_id] = 0

            if eof.processed_count == eof.expected_count:
                logging.info(f"downstreaming eof: {eof.__dict__}")
                for tx in self.txs:
                    tx.send(eof.serialize())

            else:
                logging.info(f"sending internal eof: {eof.__dict__}")
                self.mom_ring.send(eof.serialize())

            ack()
