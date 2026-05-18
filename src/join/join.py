import logging
from typing import Callable

from join_fns import JoinFn

from common.comms.messages import (
    EOF,
    MessageType,
    deserialize_message,
)
from common.comms.middleware import MessageMiddlewareQueue


class Join:
    def __init__(
        self,
        partial_res_handlers: list[tuple[MessageMiddlewareQueue, JoinFn]],
        responses_tx: MessageMiddlewareQueue,
    ):
        self.partial_res_handlers = partial_res_handlers
        self.client_responses_tx = responses_tx

    def start(self):
        for mom, join_fn in self.partial_res_handlers:
            mom.start_consuming(
                lambda bytes2, ack, nack: self._handle_message(
                    join_fn, bytes2, ack, nack
                )
            )

    def _handle_eof(self, join_fn: JoinFn, eof: EOF):
        response = join_fn.get_response(eof.client_id)
        self.client_responses_tx.send(response.serialize())

    def _handle_message(
        self, join_fn: JoinFn, bytes2: bytes, ack: Callable, nack: Callable
    ):
        msg = deserialize_message(bytes2)
        logging.debug(f"received message {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            logging.info(f"received eof {msg.__dict__}")
            self._handle_eof(join_fn, msg)  # type: ignore[reportArgumentType]
            return

        join_fn.join(msg)

        ack()
