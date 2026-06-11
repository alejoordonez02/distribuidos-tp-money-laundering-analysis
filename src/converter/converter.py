import logging
from typing import Callable, Optional

from converter_fns import ConverterFn

from common.checkpoint import Checkpointer, dispatch
from common.comms.messages import Message, deserialize_message
from common.comms.middleware import MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


class Converter:
    def __init__(
        self,
        rx: MOMQueue,
        fn: ConverterFn,
        tx: MOMQueue,
        checkpointer: Optional[Checkpointer] = None,
        input_ctx=None,
    ):
        self.rx = rx
        self.fn = fn
        self.tx = tx
        self.checkpointer = checkpointer
        self.input_ctx = input_ctx

    def start(self):
        setup_graceful_shutdown(self.stop)
        if self.checkpointer and self.checkpointer.restore():
            logging.info("restored state from checkpoint")
        self.rx.start_consuming(self._handle_message)
        self.stop()

    def stop(self):
        self.rx.stop_consuming()
        self.rx.close()
        self.tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        dispatch(
            self.checkpointer, msg, ack, self._forward_eof, self._convert, self.input_ctx
        )

    def _forward_eof(self, msg: Message):
        self.tx.send(msg.serialize())
        logging.info(f"forwarded eof for client {msg.client_id}")

    def _convert(self, msg: Message):
        self.tx.send(self.fn.convert(msg).serialize())  # type: ignore[reportAttributeAccessIssue]
