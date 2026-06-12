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
from common.fault_injection import maybe_crash

from .eof_handler import StatefulEOFHandler, total_sent
from .ring_eof_handler import RingEOFHandler

RING_LOOP_TIMEOFF = 2


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

        self.sent_data: dict[UUID, dict[int, int]] = {}
        self.confirmed_sent_data: dict[UUID, bool] = {}

    def confirm_sent_data(self, client_id: UUID):
        with self.mtx:
            self.confirmed_sent_data[client_id] = True

    def handle(self, eof: EOF):
        with self.mtx:
            eof.processed_count = self.processed_counts.get(eof.client_id, 0)
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
                client_id=eof.client_id,
                origin=self.id,
                sent_data_amount=total_sent(self.sent_data, eof.client_id),
                sent_data=self.confirmed_sent_data.get(eof.client_id, True),
                done=False,
            )
            self.sent_data.pop(eof.client_id, None)

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
        # counts are never zeroed (monotonic, checkpointed -> crash-safe); the leader
        # reseeds the lap accumulator so they are not double-added across laps
        if eof.origin == self.id:
            if eof.processed_count >= eof.expected_count:
                ring_done = RingDone(eof.client_id, self.id)
                logging.info(f"sending internal ring done: {ring_done.__dict__}")
                mom_ring_tx.send(ring_done.serialize())
                return
            with self.mtx:
                eof.processed_count = self.processed_counts.get(eof.client_id, 0)
            time.sleep(RING_LOOP_TIMEOFF)
        else:
            with self.mtx:
                eof.processed_count += self.processed_counts.get(eof.client_id, 0)

        logging.info(f"forwarding internal eof: {eof.__dict__}")
        mom_ring_tx.send(eof.serialize())
        maybe_crash("after_ring_eof_forward_before_ack")

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
        # 6. si ya terminamos limpio los recursos, y sólo corto si soy el líder
        if ring_data.done:
            logging.info(f"ring sent data done: {ring_data.__dict__}")
            self.sent_data.pop(ring_data.client_id, None)
            self.confirmed_sent_data.pop(ring_data.client_id, None)

            if ring_data.origin == self.id:
                return

            # propagate the done flag once and stop: falling through to the
            # steps below would re-emit a second (corrupted) message per node,
            # duplicating the done message every lap and looping forever.
            ring_data.sent_data = True
            logging.info(f"sending ring sent data done: {ring_data.__dict__}")
            mom_ring_tx.send(ring_data.serialize())
            return

        # 1. no importa quién sea, si alguien no terminó entonces sent data
        # es false y appendeo mi sent_data_amount al msj
        with self.mtx:
            ring_data.sent_data_amount += total_sent(self.sent_data, ring_data.client_id)
            self.sent_data.pop(ring_data.client_id, None)
            ring_data.sent_data &= self.confirmed_sent_data.get(
                ring_data.client_id, False
            )

        # 2. si no soy el "líder" forwardeo y me voy
        if self.id != ring_data.origin:
            logging.info(f"forwarding internal ring sent data: {ring_data.__dict__}")
            mom_ring_tx.send(ring_data.serialize())
            return

        # 3. si todavía no todos confirmaron que mandaron sigo girando el msj
        # y me voy
        if not ring_data.sent_data:
            ring_data.sent_data = True
            time.sleep(RING_LOOP_TIMEOFF)
            logging.info(f"restarting internal ring sent data: {ring_data.__dict__}")
            mom_ring_tx.send(ring_data.serialize())
            return

        # 4. ya confirmaron todos, sólo hace falta volver a girar el msj para
        # que ahora todos sepan que pueden liberar el recurso
        ring_data.done = True
        mom_ring_tx.send(ring_data.serialize())

        # 5. mientras todos van limpiando los recursos ya podemos ir mandando
        # el eof al próximo clúster
        eof = EOF(ring_data.client_id, expected_count=ring_data.sent_data_amount)

        logging.info("downstreaming eof: {eof.__dict__}")
        for tx in external_txs:
            tx.send(eof.serialize())
        maybe_crash("after_downstream_eof_before_ack")
