from threading import Thread
from typing import Callable

from join_fns import JoinFn
from join_route_handler import JoinRouteHandler

from common.comms.middleware import MessageMiddlewareQueue


class Join:
    def __init__(
        self,
        partial_res_handlers: list[tuple[Callable[[], MessageMiddlewareQueue], JoinFn]],
        responses_tx_factory: Callable[[], MessageMiddlewareQueue],
    ):
        self.partial_res_handlers = partial_res_handlers
        self.responses_tx_factory = responses_tx_factory

    def start(self):
        handles = []
        for mom_factory, join_fn in self.partial_res_handlers[1:]:
            route_handler = JoinRouteHandler(
                self.responses_tx_factory, mom_factory, join_fn
            )

            t = Thread(target=route_handler.start)
            t.start()

            handles.append(t)

        mom_factory, join_fn = self.partial_res_handlers[0]
        main_thread_route_handler = JoinRouteHandler(
            self.responses_tx_factory, mom_factory, join_fn
        )
        main_thread_route_handler.start()

        for t in handles:
            t.join()
