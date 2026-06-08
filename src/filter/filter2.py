import logging
from typing import Callable, Sequence

from filter_fns import FilterFn

from common.comms.eof_handler import StatelessEOFHandler
from common.comms.messages import MessageType, deserialize_message
from common.comms.middleware import MOM
from common.graceful_shutdown import setup_graceful_shutdown


class Filter:
    def __init__(
        self,
        messages_rx: MOM,
        routes: Sequence[tuple[MOM, FilterFn]],
        eof_handler: StatelessEOFHandler,
    ):
        self.messages_rx = messages_rx
        self.routes = routes
        self.eof_handler = eof_handler

    def start(self):
        setup_graceful_shutdown(self.stop)
        self.eof_handler.start()
        self.messages_rx.start_consuming(self._handle_message)
        self.stop()

    def stop(self):
        self.messages_rx.stop_consuming()
        self.eof_handler.stop()
        self.messages_rx.close()
        for tx, _ in self.routes:
            tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        logging.debug("received msg: %s", msg.__dict__)  # lazy: str() only if DEBUG

        if msg.type() == MessageType.EOF:
            self.eof_handler.handle(msg)  # type: ignore[reportUndefinedVariable]
        else:
            # TODO: tirar error si el tipo de msj no se corresponde con el `El`
            # TODO: qué pasa si se cae el filter en el medio? o sea después de
            #       haber redireccionado hacia un par de nodos
            for destination, filter_fn in self.routes:
                filtered = filter_fn.filter(msg)
                destination.send(filtered.serialize())
                logging.debug("filtered: %s", filtered.__dict__)

            self.eof_handler.add_sent_data_count(msg.client_id)
            self.eof_handler.add_processed_count(msg.client_id)

        ack()
