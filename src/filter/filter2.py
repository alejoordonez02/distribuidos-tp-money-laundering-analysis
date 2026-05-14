import logging
from typing import Callable

from filter_fns import FilterFn

from common.comms.messages import MessageType, deserialize_message
from common.comms.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue


class Filter:
    def __init__(
        self,
        messages_rx: MessageMiddlewareQueue,
        routes: list[tuple[MessageMiddlewareExchange, FilterFn]],
    ):
        self.messages_rx = messages_rx
        self.routes = routes

    def start(self):
        self.messages_rx.start_consuming(self._handle_message)

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)

        # TODO: qué pasa si se cae el filter en el medio? o sea después de
        #       haber redireccionado hacia un par de nodos
        for destination, filter_fn in self.routes:
            if filter_fn.filter(msg):
                logging.debug(f"filtered: {msg.__dict__}")
                continue

            logging.debug(f"passed: {msg.__dict__}")
            destination.send(msg.serialize())

        ack()
