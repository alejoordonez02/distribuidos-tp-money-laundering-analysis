import logging
from threading import Thread
from typing import Callable, Optional

from join_fns import JoinFn
from join_route_handler import JoinRouteHandler

from common.comms.middleware import MOM, MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


class Join:
    def __init__(
        self,
        partial_res_handlers: list[tuple[Callable[[], MOMQueue], JoinFn, int]],
        responses_tx_factory: Callable[[], MOM],
        state_dir: Optional[str] = None,
        checkpoint_every: int = 5,
    ):
        self.partial_res_handlers = partial_res_handlers
        self.responses_tx_factory = responses_tx_factory
        self._state_dir = state_dir
        self._checkpoint_every = checkpoint_every
        self._route_handlers: list[JoinRouteHandler] = []

    def _make_route_handler(self, mom_factory, join_fn, uc_id) -> JoinRouteHandler:
        return JoinRouteHandler(
            self.responses_tx_factory,
            mom_factory,
            join_fn,
            uc_id,
            self._state_dir,
            self._checkpoint_every,
        )

    def start(self):
        setup_graceful_shutdown(self.stop)
        handles = []
        for mom_factory, join_fn, uc_id in self.partial_res_handlers[1:]:
            rh = self._make_route_handler(mom_factory, join_fn, uc_id)
            self._route_handlers.append(rh)
            t = Thread(target=rh.start, daemon=True)
            t.start()
            handles.append(t)

        mom_factory, join_fn, uc_id = self.partial_res_handlers[0]
        main_rh = self._make_route_handler(mom_factory, join_fn, uc_id)
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
