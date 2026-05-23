import logging
from threading import Thread
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
        partial_res_handlers: list[tuple[Callable[[], MessageMiddlewareQueue], JoinFn]],
        responses_tx: MessageMiddlewareQueue,
    ):
        self.partial_res_handlers = partial_res_handlers
        self.client_responses_tx = responses_tx

    def start(self):
        handles = []
        for mom_factory, join_fn in self.partial_res_handlers[1:]:
            t = Thread(
                target=self._handle_route, args=[mom_factory, join_fn], daemon=True
            )
            t.start()
            handles.append(t)

        mom_factory, join_fn = self.partial_res_handlers[0]
        self._handle_route(mom_factory, join_fn)

        for t in handles:
            t.join()

    def _handle_route(
        self, mom_factory: Callable[[], MessageMiddlewareQueue], join_fn: JoinFn
    ):
        mom = mom_factory()
        mom.start_consuming(
            lambda b, ack, nack,: self._handle_message(join_fn, b, ack, nack)
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
        else:
            join_fn.join(msg)

        ack()
