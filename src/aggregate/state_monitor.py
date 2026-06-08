from collections import defaultdict
from threading import Lock
from typing import Sequence
from uuid import UUID

from aggregate_fns import StatefulFn

from common.comms.messages import Message
from common.comms.middleware import MOM


class StateMonitor:
    """
    A thread-safe wrapper for a stateful contoller's state.
    """

    def __init__(self, fn: StatefulFn):
        self._fn = fn

        self._mtx = Lock()
        self._processed_count: dict[UUID, int] = defaultdict(lambda: 0)
        self._transformed_count: dict[UUID, int] = defaultdict(lambda: 0)

    def get_transformed_count(self, client_id: UUID) -> int:
        """
        Reuturns the amount of transformed data amount for a client.

        For example, aggregating the count of some field on an input
        with of three rows of the of that field results in a 3 to 1
        processed to transformed resulting count.

        # Args
        * `client_id` - the client id for which to return the count.
        """
        with self._mtx:
            return self._transformed_count[client_id]

    def get_processed_count(self, client_id: UUID) -> int:
        """
        Returns the amount of data that entered the state and was
        processed for a client.

        # Args
        * `client_id` - the client id for which to return the count.
        """
        with self._mtx:
            return self._processed_count[client_id]

    def transform(self, msg: Message):
        """
        Locks and updates its state according to the data in the
        message.
        """
        with self._mtx:
            self._processed_count[msg.client_id] += 1
            self._fn.transform(msg)

    def send_result(self, client_id: UUID, txs: Sequence[MOM]):
        """
        Sends the result through the passed write halves.

        # Args
        * `client_id`: the id of the client whose state is going to be
          sent.
        * `txs`: the write halves for writing the data into.
        """
        with self._mtx:
            affinity_groups = self._fn.get_result(client_id)

            for aggregated, affinity in affinity_groups:
                affinity_idx = affinity % len(txs)
                txs[affinity_idx].send(aggregated.serialize())
                self._transformed_count[client_id] += 1
