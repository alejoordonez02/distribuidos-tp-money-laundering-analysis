import logging
from queue import Queue
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from common.comms.messages import EOF, RingDone, deserialize_message
from common.comms.messages.errors import UnexpectedMessageError
from common.comms.messages.message_types import MessageType
from common.comms.middleware import MOMQueue, MOMRing

from .eof_handler import StatefulEOFHandler
from .ring_eof_handler import RingEOFHandler


class StatefulRingEOFHandler(RingEOFHandler, StatefulEOFHandler):
    def __init__(
        self,
        id2: int,
        mom_ring: MOMRing,
        external_txs: list[MOMQueue],
        internal_eofs_tx: Queue[EOF],
    ):
        self.id = id2
        self.mom_ring = mom_ring
        self.external_txs = external_txs
        self.internal_eofs_tx = internal_eofs_tx

        self.processed_counts: dict[UUID, int] = {}
        self.thread_handle: Thread
        self.mtx = Lock()

        self.ring_proccessed_counts: dict[UUID, int] = {}

    def handle(self, eof: EOF):
        with self.mtx:
            # TODO: estoy lockeando porq no estoy manejando pika
            # thread-safetyness todavía
            eof.processed_count = 0
            eof.origin = self.id

            logging.info(f"sending internal eof: {eof.__dict__}")
            self.mom_ring.send(eof.serialize())

    def downstream(self, eof: EOF):
        if eof.origin != self.id:
            return

        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.external_txs:
            tx.send(eof.serialize())

    def _handle_ring_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        match msg.type():
            case MessageType.EOF:
                self._handle_ring_eof(msg)  # type: ignore[reportArgumentType]
            case MessageType.RING_DONE:
                self._handle_ring_done(msg)  # type: ignore[reportArgumentType]
            case _:
                raise UnexpectedMessageError(
                    "stateful ring eof handler received unexpected msg: {msg.__dict__}"
                )

        ack()

    def _handle_ring_eof(self, eof: EOF):
        with self.mtx:
            eof.processed_count += self.processed_counts.get(eof.client_id, 0)
            self.processed_counts[eof.client_id] = 0

            if eof.processed_count != eof.expected_count:
                logging.info(f"forwarding internal eof: {eof.__dict__}")
                self.mom_ring.send(eof.serialize())
                return

            ring_done = RingDone(eof.client_id, self.id)
            logging.info(f"sending internal ring done: {ring_done.__dict__}")
            self.mom_ring.send(ring_done.serialize())

    def _handle_ring_done(self, ring_done: RingDone):
        self.internal_eofs_tx.put(EOF(ring_done.client_id, origin=self.id))
        if ring_done.origin == self.id:
            return

        logging.info(f"forwarding internal ring done: {ring_done.__dict__}")
        self.mom_ring.send(ring_done.serialize())
