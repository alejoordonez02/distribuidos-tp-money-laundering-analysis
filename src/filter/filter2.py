import logging
from threading import Lock, Thread
from typing import Callable

from eof_handler import EOFHandler
from filter_fns import FilterFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue


class Filter:
    def __init__(
        self,
        messages_rx: MOMQueue,
        routes: list[tuple[MOMQueue, FilterFn]],
        eof_handler: EOFHandler,
    ):
        self.messages_rx = messages_rx
        self.routes = routes
        self.eof_handler = eof_handler

    def start(self):
        self.eof_handler.start()
        self.messages_rx.start_consuming(self._handle_message)

        self.messages_rx.stop_consuming()
        self.eof_handler.stop()

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        logging.debug(f"received msg: {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            self.eof_handler.handle(msg)  # type: ignore[reportUndefinedVariable]
        else:
            # TODO: tirar error si el tipo de msj no se corresponde con el `El`
            # TODO: qué pasa si se cae el filter en el medio? o sea después de
            #       haber redireccionado hacia un par de nodos
            for destination, filter_fn in self.routes:
                filtered = filter_fn.filter(msg)
                destination.send(filtered.serialize())
                logging.debug(f"filtered: {filtered.__dict__}")

            self.eof_handler.add_processed_count(msg.client_id)

        ack()
