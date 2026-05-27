import logging
from typing import Callable

from converter_fns import ConverterFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


class Converter:
    def __init__(
        self,
        rx: MOMQueue,
        fn: ConverterFn,
        tx: MOMQueue,
    ):
        self.rx = rx
        self.fn = fn
        self.tx = tx

    def start(self):
        setup_graceful_shutdown(self.stop)
        self.rx.start_consuming(self._handle_message)
        self.stop()

    def stop(self):
        self.rx.stop_consuming()
        self.rx.close()
        self.tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            self.tx.send(msg.serialize())
            logging.info(f"forwarded eof for client {msg.client_id}")
            ack()
            return

        self.tx.send(self.fn.convert(msg).serialize())  # type: ignore[reportAttributeAccessIssue]
        ack()
