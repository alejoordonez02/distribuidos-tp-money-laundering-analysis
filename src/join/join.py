import logging
from typing import Callable
from uuid import UUID

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
        routes_rx: list[tuple[MessageMiddlewareQueue, JoinFn]],
        client_responses_tx: MessageMiddlewareQueue,
    ):
        self.routes_rx = routes_rx
        self.client_responses_tx = client_responses_tx

    def start(self):
        for route, join_fn in self.routes_rx:
            route.start_consuming(
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

        if msg.type().value == MessageType.EOF.value:
            logging.info(f"received eof {msg.__dict__}")
            self._handle_eof(join_fn, msg)  # type: ignore
            return

        join_fn.join(msg)

        ack()
