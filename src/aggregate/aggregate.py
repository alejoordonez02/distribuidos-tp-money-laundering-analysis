import logging
from typing import Callable
from uuid import UUID

from aggregate_fns import AggregateFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


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
        setup_graceful_shutdown(self.stop)
        self.rx.start_consuming(self._handle_message)
        self.stop()

    def stop(self):
        self.rx.stop_consuming()
        self.rx.close()
        self.tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            eof: EOF = msg  # type: ignore[reportAssignmentType]

            count = self._eof_counts.get(eof.client_id, 0) + 1
            if count < self.npeers_upstream:
                self._eof_counts[eof.client_id] = count
                ack()
                return
            self._eof_counts.pop(eof.client_id, None)
            self.tx.send(self.fn.get_result(eof.client_id).serialize())
            # NOTE: este uno va porque ahora mismo tenemos un sólo
            #       aggregate que pushea un dato: el resultado,
            #       que va seguido del eof para el cliente.
            #       Eventualmente esto vuela porque vamos a tener
            #       el eof handler.
            eof.expected_count = 1
            self.tx.send(msg.serialize())
            logging.info(f"forwarded eof for client {msg.client_id}")
            ack()
            return

        self.fn.aggregate(msg)
        ack()
