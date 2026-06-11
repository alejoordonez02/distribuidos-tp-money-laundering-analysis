from abc import ABC, abstractmethod
from threading import Lock, Thread
from typing import Any, Callable
from uuid import UUID

from common.comms.middleware import MOMRing

from .eof_handler import restore_counts, snapshot_counts


class RingEOFHandler(ABC):
    mom_ring: MOMRing
    mtx: Lock
    processed_counts: dict[UUID, int]
    sent_data: dict[UUID, int]

    def start(self):
        self.thread_handle = Thread(target=self._start_consuming_back)
        self.thread_handle.start()

    def snapshot_state(self) -> dict[str, Any]:
        with self.mtx:
            return snapshot_counts(self.processed_counts, self.sent_data)

    def restore_state(self, snapshot: dict[str, Any]):
        with self.mtx:
            self.processed_counts, self.sent_data = restore_counts(snapshot)

    def stop(self):
        self.mom_ring.stop_consuming()
        self.thread_handle.join()

    def add_processed_count(self, client_id: UUID):
        with self.mtx:
            if client_id not in self.processed_counts:
                self.processed_counts[client_id] = 0

            self.processed_counts[client_id] += 1

    def add_sent_data_count(self, client_id: UUID):
        # locked: the controller thread increments while the ring thread reads it
        with self.mtx:
            if client_id not in self.sent_data:
                self.sent_data[client_id] = 0

            self.sent_data[client_id] += 1

    def _start_consuming_back(self):
        exclusive = self.mom_ring.clone()
        exclusive.start_consuming(
            lambda bytes2, ack, nack: self._handle_ring_message(
                bytes2, ack, nack, exclusive
            )
        )
        exclusive.stop_consuming()

    @abstractmethod
    def _handle_ring_message(
        self, bytes2: bytes, ack: Callable, nack: Callable, mom_ring_tx: MOMRing
    ): ...
