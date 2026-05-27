from abc import ABC
from threading import Lock, Thread
from typing import Callable, Self
from uuid import UUID

from common.comms.middleware import MOMRing


# This class serves only as a base implementation for ring
# eof handlers common methods with inheritance.
class RingEOFHandler(ABC):
    mom_ring: MOMRing
    mtx: Lock
    processed_counts: dict[UUID, int]
    _handle_ring_eof: Callable[[Self, bytes, Callable, Callable], None]

    def start(self):
        self.thread_handle = Thread(
            target=self.mom_ring.start_consuming, args=(self._handle_ring_eof,)
        )
        self.thread_handle.start()

    def stop(self):
        self.mom_ring.stop_consuming()
        self.thread_handle.join()

    def add_processed_count(self, client_id: UUID):
        with self.mtx:
            if client_id not in self.processed_counts:
                self.processed_counts[client_id] = 0

            self.processed_counts[client_id] += 1
