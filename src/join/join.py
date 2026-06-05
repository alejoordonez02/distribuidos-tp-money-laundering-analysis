import logging
from threading import Lock, Thread
from typing import Callable

from join_fns import JoinFn
from join_route_handler import JoinRouteHandler

from common.comms.middleware import MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


class Join:
    def __init__(
        self,
        partial_res_handlers: list[tuple[Callable[[], MOMQueue], JoinFn]],
        responses_tx_factory: Callable[[], MOMQueue],
    ):
        self.partial_res_handlers = partial_res_handlers
        self.responses_tx_factory = responses_tx_factory
        self._route_handlers: list[JoinRouteHandler] = []

    def start(self):
        setup_graceful_shutdown(self.stop)
        # Shared across all route handlers so each UC's chunks stay contiguous in
        # the responses queue.
        responses_lock = Lock()
        handles = []
        for mom_factory, join_fn in self.partial_res_handlers[1:]:
            rh = JoinRouteHandler(self.responses_tx_factory, mom_factory, join_fn, responses_lock)
            self._route_handlers.append(rh)
            t = Thread(target=rh.start, daemon=True)
            t.start()
            handles.append(t)

        mom_factory, join_fn = self.partial_res_handlers[0]
        main_rh = JoinRouteHandler(self.responses_tx_factory, mom_factory, join_fn, responses_lock)
        self._route_handlers.append(main_rh)
        try:
            main_rh.start()
        except Exception as e:
            # TODO: handle specific exceptions from JoinRouteHandler
            logging.error("!!! UNHANDLED exception in join main route handler: %s", e, exc_info=True)

        for t in handles:
            t.join()

        self.stop()

    def stop(self):
        for rh in self._route_handlers:
            rh.stop()
            rh.close()
