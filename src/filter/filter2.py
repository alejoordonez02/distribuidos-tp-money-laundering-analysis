import logging
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from filter_fns import FilterFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue, MOMRing


class Filter:
    def __init__(
        self,
        ring_mom: MOMRing,
        messages_rx: MOMQueue,
        routes: list[tuple[MOMQueue, FilterFn]],
    ):
        self.processed_counts: dict[UUID, int] = {}
        self.messages_rx = messages_rx
        self.routes = routes

        self.ring_mom = ring_mom
        self.ring_handle: Thread
        self.mtx = Lock()

    def start(self):
        self.ring_handle = Thread(
            target=self.ring_mom.start_consuming, args=(self._handle_ring_message,)
        )

        self.ring_handle.start()
        self.messages_rx.start_consuming(self._handle_message)

        self.ring_handle.join()

    def _handle_eof(self, eof: EOF):
        # TODO: esto obviamente es temporal xd
        if not self.ring_mom.peer_ids:  # type: ignore[reportAttributeAccessIssue]
            logging.info(f"downstreaming eof: {eof.__dict__}")
            for destination, _ in self.routes:
                destination.send(eof.serialize())
            return

        with self.mtx:
            # TODO: estoy lockeando porq no estoy manejando pika
            # thread-safetyness todavía
            eof.processed_count = 0
            self.ring_mom.send(eof.serialize())

    def _handle_ring_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        eof: EOF = EOF.deserialize(bytes2)  # type: ignore[reportAssignmentType]

        with self.mtx:
            eof.processed_count += self.processed_counts.get(eof.client_id, 0)
            self.processed_counts[eof.client_id] = 0

            if eof.processed_count == eof.expected_count:
                logging.info(f"downstreaming eof: {eof.__dict__}")
                for destination, _ in self.routes:
                    destination.send(eof.serialize())
            else:
                self.ring_mom.send(eof.serialize())

            ack()

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        logging.debug(f"received msg: {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            self._handle_eof(msg)  # type: ignore
        else:
            self.processed_counts
            # TODO: tirar error si el tipo de msj no se corresponde con el `El`
            # TODO: qué pasa si se cae el filter en el medio? o sea después de
            #       haber redireccionado hacia un par de nodos
            for destination, filter_fn in self.routes:
                filtered = filter_fn.filter(msg)
                destination.send(filtered.serialize())
                logging.debug(f"filtered: {filtered.__dict__}")

            with self.mtx:
                if msg.client_id not in self.processed_counts:
                    self.processed_counts[msg.client_id] = 0

                self.processed_counts[msg.client_id] += 1

        ack()
