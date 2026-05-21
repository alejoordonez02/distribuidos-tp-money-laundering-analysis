import logging
from typing import Callable

from converter_fns import ConverterFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MessageMiddlewareQueue


class Converter:
    def __init__(
        self,
        rx: MessageMiddlewareQueue,
        fn: ConverterFn,
        tx: MessageMiddlewareQueue,
    ):
        self.rx = rx
        self.fn = fn
        self.tx = tx

    def start(self):
        self.rx.start_consuming(self._handle_message)

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            self.tx.send(msg.serialize())
            logging.info(f"forwarded eof for client {msg.client_id}")
            return

        self.tx.send(self.fn.convert(msg).serialize())  # type: ignore[reportAttributeAccessIssue]
        ack()
