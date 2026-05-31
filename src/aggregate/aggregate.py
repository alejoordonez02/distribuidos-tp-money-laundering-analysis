import logging
from queue import Queue
from threading import Thread
from typing import Callable, Sequence

from aggregate_fns import AggregateFn

from common.comms.eof_handler.eof_handler import StatefulEOFHandler
from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOM
from common.graceful_shutdown import setup_graceful_shutdown


class Aggregate:
    def __init__(
        self,
        fn: AggregateFn,
        external_rx: MOM,
        external_txs: Sequence[MOM],
        eof_handler: StatefulEOFHandler,
        internal_eofs_rx: Queue[EOF],
    ):
        self.external_rx = external_rx
        self.fn = fn
        self.external_txs = external_txs
        self.eof_handler = eof_handler
        self.internal_eofs_rx = internal_eofs_rx

        self._should_keep_running = False
        self.internal_eofs_handle: Thread

    def start(self):
        setup_graceful_shutdown(self.stop)
        self._should_keep_running = True
        # TODO: estaría bueno no levantar este thread cuando estamos
        #       sólos, normalmente me molestaría más pero por como se
        #       manejan los threads en python la verdad prefiero que
        #       esté programado más prolijo a no tener este """thread"""
        #       al pedo.
        self.internal_eofs_handle = Thread(target=self._handle_internal_eofs)
        self.internal_eofs_handle.start()
        self.eof_handler.start()
        self.external_rx.start_consuming(self._handle_message)

    def stop(self):
        self.external_rx.stop_consuming()
        self.external_rx.close()
        for tx in self.external_txs:
            tx.close()

        self.eof_handler.stop()
        self.internal_eofs_rx.shutdown()
        self.internal_eofs_handle.join()

    def _handle_internal_eofs(self):
        while self._should_keep_running:
            try:
                eof = self.internal_eofs_rx.get(block=True)
            except Exception as e:
                # TODO: mejor catchear la excepción más
                #       específica pero no tengo ganas
                #       de buscar cuál es (supongo OS)
                if self._should_keep_running:
                    raise e
                return

            affinity_groups = self.fn.get_result(eof.client_id)

            for aggregated, affinity in affinity_groups:
                external_tx_idx = affinity % len(self.external_txs)
                self.external_txs[external_tx_idx].send(aggregated.serialize())

                # FIXME: ver q esto funcione en todos
                #       los eof handlers
                self.eof_handler.add_next_expected_processed_counts(eof.client_id)

            # NOTE: the eof that's passed to `downstream`
            #       must be the same one that's popped
            #       from the `internal_eofs` queue.
            self.eof_handler.downstream(eof)

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        logging.debug(f"received msg: {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            self.eof_handler.handle(msg)  # type: ignore[reportArgumentType]
        else:
            self.fn.aggregate(msg)
            self.eof_handler.add_processed_count(msg.client_id)

        ack()
