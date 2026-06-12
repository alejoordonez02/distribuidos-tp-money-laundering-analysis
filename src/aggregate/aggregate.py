import logging
from contextlib import nullcontext
from queue import Queue
from threading import Thread
from typing import Callable, Optional, Sequence

from aggregate_fns import AggregateFn

from common.checkpoint import Checkpointer
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
        checkpointer: Optional[Checkpointer] = None,
        broadcast_downstream: bool = False,
    ):
        self.external_rx = external_rx
        self.fn = fn
        self.external_txs = external_txs
        self.eof_handler = eof_handler
        self.internal_eofs_rx = internal_eofs_rx
        self.checkpointer = checkpointer
        # broadcast: send every result to ALL downstream txs (a small global state
        # fanned out to N broadcast-join replicas) instead of sharding by affinity.
        self.broadcast_downstream = broadcast_downstream

        self._should_keep_running = False
        self.internal_eofs_handle: Thread

    def start(self):
        setup_graceful_shutdown(self.stop)
        self._should_keep_running = True
        if self.checkpointer and self.checkpointer.restore():
            logging.info("restored state from checkpoint")
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

            lock = self.checkpointer.lock if self.checkpointer else nullcontext()
            with lock:
                affinity_groups = list(self.fn.get_result(eof.client_id))

            for aggregated, affinity in affinity_groups:
                if self.broadcast_downstream:
                    # every replica gets the full state; count once (each tx gets all)
                    for tx in self.external_txs:
                        tx.send(aggregated.serialize())
                    self.eof_handler.add_sent_data_count(eof.client_id)
                    continue
                external_tx_idx = affinity % len(self.external_txs)
                self.external_txs[external_tx_idx].send(aggregated.serialize())

                self.eof_handler.add_sent_data_count(eof.client_id, external_tx_idx)

            self.eof_handler.confirm_sent_data(eof.client_id)

            # NOTE: the eof that's passed to `downstream`
            #       must be the same one that's popped
            #       from the `internal_eofs` queue.
            self.eof_handler.downstream(eof)

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        logging.debug("received msg: %s", msg.__dict__)  # lazy: str() only if DEBUG

        if msg.type() == MessageType.EOF:
            if self.checkpointer:
                self.checkpointer.flush()
            self.eof_handler.handle(msg)  # type: ignore[reportArgumentType]
            ack()
            return

        if self.checkpointer is None:
            self.fn.aggregate(msg)
            self.eof_handler.add_processed_count(msg.client_id)
            ack()
            return

        def apply():
            self.fn.aggregate(msg)
            self.eof_handler.add_processed_count(msg.client_id)

        self.checkpointer.handle_data(msg, apply, ack)
