from abc import ABC, abstractmethod
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from common.comms.middleware import MOMRing


# This class serves only as a base implementation for ring
# eof handlers common methods with inheritance.
class RingEOFHandler(ABC):
    mom_ring: MOMRing
    mtx: Lock
    processed_counts: dict[UUID, int]
    sent_data: dict[UUID, int]

    def start(self):
        self.thread_handle = Thread(target=self._start_consuming_back)
        self.thread_handle.start()

    def stop(self):
        self.mom_ring.stop_consuming()
        self.thread_handle.join()

    def add_processed_count(self, client_id: UUID):
        with self.mtx:
            if client_id not in self.processed_counts:
                self.processed_counts[client_id] = 0

            self.processed_counts[client_id] += 1

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
