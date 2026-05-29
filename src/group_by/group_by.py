from queue import Queue
from threading import Thread
from typing import Callable, Sequence
from uuid import UUID

from group_by_fns import GroupByFn

from common.comms.eof_handler import StatefulEOFHandler
from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware.mom import MOM
from common.graceful_shutdown import setup_graceful_shutdown


class GroupBy:
    def __init__(
        self,
        external_rx: MOM,
        fn: GroupByFn,
        external_tx: Sequence[MOM],
        eof_handler: StatefulEOFHandler,
        internal_eofs_rx: Queue[EOF],
        npeers_upstream: int = 1,
    ):
        self.external_rx = external_rx
        self.fn = fn
        self.external_txs = external_tx
        self.eof_handler = eof_handler
        self.internal_eofs_rx = internal_eofs_rx
        self.npeers_upstream = npeers_upstream
        self._upstream_eof_counts: dict[UUID, int] = {}

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

            maybe_w_affinity = self.fn.get_result(eof.client_id)
            if isinstance(maybe_w_affinity, tuple):
                result, affinity = maybe_w_affinity
            else:
                result = maybe_w_affinity
                affinity = 0

            external_tx_idx = affinity % len(self.external_txs)

            self.external_txs[external_tx_idx].send(result.serialize())
            # NOTE: the eof that's passed to `downstream`
            #       must be the same one that's popped
            #       from the `internal_eofs` queue.
            self.eof_handler.downstream(eof)

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            count = self._upstream_eof_counts.get(msg.client_id, 0) + 1
            if count < self.npeers_upstream:
                self._upstream_eof_counts[msg.client_id] = count
            else:
                self._upstream_eof_counts.pop(msg.client_id, None)
                self.eof_handler.handle(msg)  # type: ignore[reportArgumentType]
        else:
            self.fn.group_by(msg)
            self.eof_handler.add_processed_count(msg.client_id)

        ack()
