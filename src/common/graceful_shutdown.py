import faulthandler
from signal import SIGINT, SIGTERM, SIGUSR1, signal
from typing import Callable


def setup_graceful_shutdown(stop: Callable[[], None]) -> None:
    def _handler(signum, frame):
        stop()

    signal(SIGTERM, _handler)
    signal(SIGINT, _handler)
    faulthandler.register(SIGUSR1, all_threads=True)
