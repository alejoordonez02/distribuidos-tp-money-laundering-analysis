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
        # NOTE: estos quizás habría que clonarlos
        #       no puedo pensar mucho más así que
        #       me limito a dejar este comentario.
        #       No me gustaría clonarlos
        #       innecesariamente...
        #       Creo que la razón por la cuál no
        #       es necesario es que a diferencia
        #       de en stateless eof handler, acá
        #       el thread que los usa es el mismo
        #       thread en el que se está corriendo
        #       el dueño de este componente,
        #       es ahí donde se decide cuándo
        #       mandar el eof llamando a
        #       `downstream`.
        self.mom_ring = mom_ring
        self.external_txs = external_txs
        self.internal_eofs_tx = internal_eofs_tx

        self.processed_counts: dict[UUID, int] = {}
        self.thread_handle: Thread
        self.mtx = Lock()

        self.ring_proccessed_counts: dict[UUID, int] = {}

    def handle(self, eof: EOF):
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

    def _handle_ring_message(
        self, bytes2: bytes, ack: Callable, nack: Callable, mom_ring_tx: MOMRing
    ):
        msg = deserialize_message(bytes2)
        match msg.type():
            case MessageType.EOF:
                self._handle_ring_eof(msg, mom_ring_tx)  # type: ignore[reportArgumentType]
            case MessageType.RING_DONE:
                self._handle_ring_done(msg, mom_ring_tx)  # type: ignore[reportArgumentType]
            case _:
                raise UnexpectedMessageError(
                    "stateful ring eof handler received unexpected msg: {msg.__dict__}"
                )

        ack()

    def _handle_ring_eof(self, eof: EOF, mom_ring_tx: MOMRing):
        with self.mtx:
            eof.processed_count += self.processed_counts.get(eof.client_id, 0)
            self.processed_counts[eof.client_id] = 0

            if eof.processed_count != eof.expected_count:
                logging.info(f"forwarding internal eof: {eof.__dict__}")
                mom_ring_tx.send(eof.serialize())
                return

            ring_done = RingDone(eof.client_id, self.id)
            logging.info(f"sending internal ring done: {ring_done.__dict__}")
            mom_ring_tx.send(ring_done.serialize())

    def _handle_ring_done(self, ring_done: RingDone, mom_ring_tx: MOMRing):
        self.internal_eofs_tx.put(EOF(ring_done.client_id, origin=ring_done.origin))
        if ring_done.origin == self.id:
            return

        logging.info(f"forwarding internal ring done: {ring_done.__dict__}")
        mom_ring_tx.send(ring_done.serialize())
