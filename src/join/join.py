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
        partial_res_handlers: list[tuple[MessageMiddlewareQueue, JoinFn]],
        responses_tx: MessageMiddlewareQueue,
    ):
        self.partial_res_handlers = partial_res_handlers
        self.client_responses_tx = responses_tx

    def start(self):
        if len(self.partial_res_handlers) == 1:
            mom, join_fn = self.partial_res_handlers[0]
            mom.start_consuming(
                lambda b, ack, nack: self._handle_message(join_fn, b, ack, nack)
            )
            return

        # multiples colas: cada una corre en su propio thread. Joinfn tiene que ser thread-safe pa.
        threads = []
        for mom, join_fn in self.partial_res_handlers[:-1]:
            t = Thread(
                target=mom.start_consuming,
                args=[
                    lambda b, ack, nack, fn=join_fn: self._handle_message(
                        fn, b, ack, nack
                    )
                ],
                daemon=True,
            )
            t.start()
            threads.append(t)

        mom, join_fn = self.partial_res_handlers[-1]
        mom.start_consuming(
            lambda b, ack, nack: self._handle_message(join_fn, b, ack, nack)
        )

        for t in threads:
            t.join()

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
