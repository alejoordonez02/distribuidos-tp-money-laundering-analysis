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
        # NOTE: esto está un poco feo, porque estoy esperando
        #       que me manden el mismo eof por acá que el que
        #       estoy mandando yo por internal_eof_tx, de
        #       última lo puedo aclarar bien en la docu.
        if eof.origin != self.id:
            return

        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.external_txs:
            tx.send(eof.serialize())

    def _handle_ring_eof(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)

        match msg.type():
            case MessageType.EOF:
                eof: EOF = msg  # type: ignore[reportAssignmentType]

                with self.mtx:
                    eof.processed_count += self.processed_counts.get(eof.client_id, 0)
                    self.processed_counts[eof.client_id] = 0

                    # me falta mandar el internal eof con processed completo para
                    # que los peers puedan llamar
                    if eof.processed_count == eof.expected_count:
                        # self.internal_eofs_tx.put(eof)

                        ring_done = RingDone(eof.client_id, self.id)
                        logging.info(
                            f"sending internal ring done: {ring_done.__dict__}"
                        )
                        self.mom_ring.send(ring_done.serialize())

                    else:  # eof.processed_count < eof.expected_count, esto no sé
                        logging.info(f"forwarding internal eof: {eof.__dict__}")
                        self.mom_ring.send(eof.serialize())

            case MessageType.RING_DONE:
                ring_done: RingDone = msg  # type: ignore[reportAssignmentType]

                # TODO: la verdad podría pushear directamente los
                #       client ids por acá en vez del eof

                eof = EOF(ring_done.client_id, origin=self.id)
                if ring_done.origin != self.id:
                    # acá no voy a estar mandando eof porque no va a dar el if!
                    # sólo voy a estar mandando la data
                    self.internal_eofs_tx.put(eof)
                    logging.info(f"forwarding internal ring done: {ring_done.__dict__}")
                    self.mom_ring.send(ring_done.serialize())
                else:
                    # lo que estoy haciendo acá es asegurarme que yo, que soy el
                    # origen, sea quien manda el eof al próximo cluster, porque en
                    # el downstream está el if!
                    self.internal_eofs_tx.put(eof)

            case _:
                raise UnexpectedMessageError(
                    "stateful ring eof handler received unexpected msg: {msg.__dict__}"
                )

        ack()
