from threading import Thread
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
        handles = []
        for mom_factory, join_fn in self.partial_res_handlers[1:]:
            rh = JoinRouteHandler(self.responses_tx_factory, mom_factory, join_fn)
            self._route_handlers.append(rh)
            t = Thread(target=rh.start)
            t.start()
            handles.append(t)

        mom_factory, join_fn = self.partial_res_handlers[0]
        main_rh = JoinRouteHandler(self.responses_tx_factory, mom_factory, join_fn)
        self._route_handlers.append(main_rh)
        main_rh.start()

        for t in handles:
            t.join()

        for rh in self._route_handlers:
            rh.close()

    def stop(self):
        for rh in self._route_handlers:
            rh.stop()
