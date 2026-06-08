import logging
from typing import Callable, Sequence

from group_by_fns import GroupByFn

from common.comms.eof_handler import StatelessEOFHandler
from common.comms.messages import MessageType, deserialize_message
from common.comms.middleware import MOM
from common.graceful_shutdown import setup_graceful_shutdown


class GroupBy:
    def __init__(
        self,
        fn: GroupByFn,
        external_rx: MOM,
        external_txs: Sequence[MOM],
        eof_handler: StatelessEOFHandler,
    ):
        self.external_rx = external_rx
        self.external_txs = external_txs
        self.fn = fn
        self.eof_handler = eof_handler

    def start(self):
        setup_graceful_shutdown(self.stop)
        self.eof_handler.start()
        self.external_rx.start_consuming(self._handle_message)
        self.stop()

    def stop(self):
        self.external_rx.stop_consuming()
        self.eof_handler.stop()
        self.external_rx.close()
        for tx in self.external_txs:
            tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        logging.debug("received msg: %s", msg.__dict__)  # lazy: str() only if DEBUG

        if msg.type() == MessageType.EOF:
            self.eof_handler.handle(msg)  # type: ignore[reportUndefinedVariable]
        else:
            affinity_groups = self.fn.group_by(msg)

            for group, affinity in affinity_groups:
                external_tx_idx = affinity % len(self.external_txs)
                self.external_txs[external_tx_idx].send(group.serialize())

                # FIXME: ver q esto funcione en todos
                #       los eof handlers
                self.eof_handler.add_sent_data_count(msg.client_id)

            self.eof_handler.add_processed_count(msg.client_id)

        ack()
