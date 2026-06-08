import logging
import time
from queue import Queue
from threading import Thread
from typing import Callable, Sequence

from aggregate_fns import StatefulFn
from state_monitor import StateMonitor

from common.comms.eof_handler import StatefulEOFHandler
from common.comms.messages import (
    EOF,
    Message,
    MessageType,
    RingDone,
    RingSentData,
    deserialize_message,
)
from common.comms.middleware import MOM, MOMRing
from common.graceful_shutdown import setup_graceful_shutdown

RING_LOOP_TIMEOFF = 2


class StatefulRingHandler:
    def __init__(
        self,
        id2: int,
        mom_ring: MOMRing,
        external_txs: Sequence[MOM],
        state: StateMonitor,
    ):
        self.id = id2
        self.mom_ring = mom_ring
        self.external_txs = external_txs
        self.state = state

        self._should_keep_running = False

    def handle_ring_message(self, msg: Message):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def handle_external_eof(self, eof: EOF):
        raise NotImplementedError

    def downstream(self, eof: EOF):
        # si no soy el "líder" de esta vuelta me voy
        if eof.origin != self.id:
            return

        # appendeo la data que mandé yo y lo hago girar, eventualmente
        # me va a llegar a mí con todo sumado
        ring_data = RingSentData(
            client_id=eof.client_id,
            origin=self.id,
            sent_data_amount=self.state.pop_sent_count(eof.client_id),
            sent_data=self.state.get_confirmed(
                eof.client_id  # FIXME: el default de esto era True
            ),
            done=False,
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

    # TODO: toda esta indirección es de lo peor que hice en mi vida pero
    #       estoy apurado
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
        processed_count = self.state.pop_processed_count(eof.client_id)
        eof.processed_count = processed_count

        if eof.processed_count != eof.expected_count:
            if eof.origin == self.id:
                # avoid making the eof loop too much
                time.sleep(RING_LOOP_TIMEOFF)

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
        # 6. si ya terminamos limpio los recursos
        if ring_data.done:
            logging.info(f"received ring sent data done: {ring_data.__dict__}")
            self.state.clear(ring_data.client_id)

            # si soy el líder corto
            if ring_data.origin == self.id:
                return

            # si no soy el líder reenvío
            ring_data.sent_data = True
            logging.info(f"forwarding ring sent data done: {ring_data.__dict__}")
            mom_ring_tx.send(ring_data.serialize())
            return

        # 1. no importa quién sea, si alguien no terminó entonces sent
        #    data es false y appendeo mi sent_data_amount al msj
        with self.mtx:
            ring_data.sent_data_amount += self.sent_data.pop(ring_data.client_id, 0)
            ring_data.sent_data &= self.state.get_confirmed(ring_data.client_id)

        # 2. si no soy el "líder" forwardeo y me voy
        if self.id != ring_data.origin:
            logging.info(f"forwarding ring sent data: {ring_data.__dict__}")
            mom_ring_tx.send(ring_data.serialize())
            return

        # 3. si todavía no todos confirmaron que mandaron sigo girando
        #    el msj y me voy
        if not ring_data.sent_data:
            ring_data.sent_data = True
            time.sleep(RING_LOOP_TIMEOFF)
            logging.info(f"restarting ring sent data: {ring_data.__dict__}")
            mom_ring_tx.send(ring_data.serialize())
            return

        # 4. ya confirmaron todos, sólo hace falta volver a girar el msj
        #    para que ahora todos sepan que pueden liberar el recurso
        ring_data.done = True
        logging.info(f"sending ring sent data done: {ring_data.__dict__}")
        mom_ring_tx.send(ring_data.serialize())

        # 5. mientras todos van limpiando los recursos ya podemos ir
        #    mandando el eof al próximo clúster
        eof = EOF(ring_data.client_id, expected_count=ring_data.sent_data_amount)

        logging.info("downstreaming eof: {eof.__dict__}")
        for tx in external_txs:
            tx.send(eof.serialize())


class StatefulController:
    def __init__(
        self,
        external_rx: MOM,
        external_txs: Sequence[MOM],
        state: StateMonitor,
        ring_handler: StatefulRingHandler,
    ):
        self.external_rx = external_rx
        self.external_txs = external_txs
        self.state = state
        self.ring_handler = ring_handler

        self._should_keep_running = False
        self._ring_handle: Thread

    def start(self):
        setup_graceful_shutdown(self.stop)
        self._should_keep_running = True

        self._ring_handle = Thread(target=self._ring_handle.start())
        self.external_rx.start_consuming(self._handle_message)

    def stop(self):
        self.external_rx.stop_consuming()
        self.external_rx.close()
        for tx in self.external_txs:
            tx.close()

        self.ring_handler.stop()
        self._ring_handle.join()

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        logging.debug("received msg: %s", msg.__dict__)

        if msg.type() == MessageType.EOF:
            eof: EOF = msg  # type: ignore[reportAssignmentType]
            self.ring_handler.handle_external_eof(eof)
        else:
            self.state.transform(msg)
            self.eof_handler.add_processed_count(msg.client_id)

        ack()
