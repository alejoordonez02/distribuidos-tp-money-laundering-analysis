import logging
from typing import Callable

from filter_fns import FilterFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue


class Filter:
    def __init__(
        self,
        messages_rx: MOMQueue,
        routes: list[tuple[MOMQueue, FilterFn]],
    ):
        self.messages_rx = messages_rx
        self.routes = routes

    def start(self):
        self.messages_rx.start_consuming(self._handle_message)

    def _handle_eof(self, eof: EOF):
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for destination, _ in self.routes:
            destination.send(eof.serialize())

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        logging.debug(f"received msg: {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            self._handle_eof(msg)  # type: ignore
        else:
            # TODO: tirar error si el tipo de msj no se corresponde con el `El`
            # TODO: qué pasa si se cae el filter en el medio? o sea después de
            #       haber redireccionado hacia un par de nodos
            for destination, filter_fn in self.routes:
                filtered = filter_fn.filter(msg)
                if hasattr(filtered, "transactions") and not filtered.transactions:  # type: ignore[reportAttributeAccessIssue]
                    continue
                destination.send(filtered.serialize())
                logging.debug(f"filtered: {filtered.__dict__}")

        ack()
