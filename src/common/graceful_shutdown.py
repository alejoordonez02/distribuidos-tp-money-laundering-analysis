import faulthandler
from signal import SIGINT, SIGTERM, SIGUSR1, signal
from typing import Callable


def setup_graceful_shutdown(stop: Callable[[], None]) -> None:
    def _handler(signum, frame):
        stop()

    signal(SIGTERM, _handler)
    signal(SIGINT, _handler)
    # SIGUSR1 dumps all thread stacks for live debugging
    faulthandler.register(SIGUSR1, all_threads=True)
