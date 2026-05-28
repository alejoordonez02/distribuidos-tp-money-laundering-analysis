import logging
from typing import Callable
from uuid import UUID

from aggregate_fns import AggregateFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue


class Aggregate:
    def __init__(
        self,
        rx: MOMQueue,
        fn: AggregateFn,
        tx: MOMQueue,
        npeers_upstream: int = 1,
    ):
        self.rx = rx
        self.fn = fn
        self.tx = tx
        self.npeers_upstream = npeers_upstream
        self._eof_counts: dict[UUID, int] = {}

    def start(self):
        self.rx.start_consuming(self._handle_message)

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            count = self._eof_counts.get(msg.client_id, 0) + 1
            if count < self.npeers_upstream:
                self._eof_counts[msg.client_id] = count
                ack()
                return
            self._eof_counts.pop(msg.client_id, None)
            self.tx.send(self.fn.get_result(msg.client_id).serialize())
            self.tx.send(EOF(msg.client_id).serialize())
            logging.info(f"forwarded eof for client {msg.client_id}")
            ack()
            return

        self.fn.aggregate(msg)
        ack()
