import logging
from typing import Callable

from group_by_fns import GroupByFn

from common.comms.eof_handler.eof_handler import StatelessEOFHandler
from common.comms.messages import MessageType, deserialize_message
from common.comms.middleware import MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


class GroupBy:
    def __init__(
        self,
        fn: GroupByFn,
        external_rx: MOMQueue,
        external_tx: MOMQueue,
        eof_handler: StatelessEOFHandler,
    ):
        self.external_rx = external_rx
        self.external_tx = external_tx
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
        self.external_tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        logging.debug(f"received msg: {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            self.eof_handler.handle(msg)  # type: ignore[reportUndefinedVariable]
        else:
            grouped = self.fn.group_by(msg)
            self.external_tx.send(grouped.serialize())
            self.eof_handler.add_processed_count(msg.client_id)

        ack()
