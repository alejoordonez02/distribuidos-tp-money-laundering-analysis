import logging
import time
from queue import Queue
from threading import Lock
from typing import Callable, Sequence
from uuid import UUID

from common.comms.messages import EOF, RingDone, RingSentData, deserialize_message
from common.comms.messages.errors import UnexpectedMessageError
from common.comms.messages.message_types import MessageType
from common.comms.middleware import MOM, MOMRing

from .eof_handler import StatefulEOFHandler
from .ring_eof_handler import RingEOFHandler

EOF_LOOP_SECS = 2


class StatefulRingEOFHandler(RingEOFHandler, StatefulEOFHandler):
    def __init__(
        self,
        id2: int,
        mom_ring: MOMRing,
        external_txs: Sequence[MOM],
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
        self.mtx = Lock()

        self.sent_data: dict[UUID, int] = {}

    def handle(self, eof: EOF):
        eof.processed_count = 0
        eof.next_expected_count = 0
        eof.origin = self.id

        logging.info(f"sending internal eof: {eof.__dict__}")
        self.mom_ring.send(eof.serialize())

    def downstream(self, eof: EOF):
        # si no soy el "líder" de esta vuelta me voy
        if eof.origin != self.id:
            return

        # appendeo la data que mandé yo y lo hago girar,
        # eventualmente me va a llegar a mí con todo sumado
        with self.mtx:
            ring_data = RingSentData(
                eof.client_id, self.id, self.sent_data.pop(eof.client_id, 0)
            )

        logging.info(f"sending internal ring sent data: {ring_data.__dict__}")
        self.mom_ring.send(ring_data.serialize())

    def _start_consuming_back(self):
        exclusive_mom_ring = self.mom_ring.clone()
        exlusive_external_txs = [tx.clone() for tx in self.external_txs]
        exclusive_mom_ring.start_consuming(
            lambda bytes2, ack, nack: self._handle_ring_message(
                bytes2, ack, nack, exclusive_mom_ring, exlusive_external_txs
            )
        )
        exclusive_mom_ring.stop_consuming()

    # TODO: toda esta indirección es de lo peor que
    #       hice en mi vida pero estoy apurado
    def _handle_ring_message(  # type: ignore[reportIncompatibleMethodOverride]
        self,
        bytes2: bytes,
        ack: Callable,
        _: Callable,
        mom_ring_tx: MOMRing,
        external_txs: Sequence[MOM],
    ):
        msg = deserialize_message(bytes2)
        match msg.type():
            case MessageType.EOF:
                self._handle_ring_eof(msg, mom_ring_tx)  # type: ignore[reportArgumentType]
            case MessageType.RING_DONE:
                self._handle_ring_done(msg, mom_ring_tx)  # type: ignore[reportArgumentType]
            case MessageType.RING_SENT_DATA:
                self._handle_ring_sent_data(msg, mom_ring_tx, external_txs)  # type: ignore[reportArgumentType]
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
            if eof.origin == self.id:
                time.sleep(EOF_LOOP_SECS)  # avoid making the eof loop too much

            logging.info(f"forwarding internal eof: {eof.__dict__}")
            mom_ring_tx.send(eof.serialize())
            return

        ring_done = RingDone(eof.client_id, self.id)
        logging.info(f"sending internal ring done: {ring_done.__dict__}")
        mom_ring_tx.send(ring_done.serialize())

    def _handle_ring_done(self, ring_done: RingDone, mom_ring_tx: MOMRing):
        self.internal_eofs_tx.put(
            EOF(client_id=ring_done.client_id, origin=ring_done.origin)
        )

        # da una sola vuelta
        if ring_done.origin == self.id:
            return

        logging.info(f"forwarding internal ring done: {ring_done.__dict__}")
        mom_ring_tx.send(ring_done.serialize())

    def _handle_ring_sent_data(
        self, ring_data: RingSentData, mom_ring_tx: MOMRing, external_txs: Sequence[MOM]
    ):
        if self.id != ring_data.origin:
            ring_data.sent_data += self.sent_data.pop(ring_data.client_id, 0)
            logging.info(f"forwarding internal ring sent data: {ring_data.__dict__}")
            mom_ring_tx.send(ring_data.serialize())
            return

        eof = EOF(ring_data.client_id, expected_count=ring_data.sent_data)

        logging.info("downstreaming eof: {eof.__dict__}")
        for tx in external_txs:
            tx.send(eof.serialize())
