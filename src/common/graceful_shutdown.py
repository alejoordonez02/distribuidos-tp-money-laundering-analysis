from signal import SIGINT, SIGTERM, signal
from typing import Callable


def setup_graceful_shutdown(stop: Callable[[], None]) -> None:
    def _handler(signum, frame):
        stop()

    signal(SIGTERM, _handler)
    signal(SIGINT, _handler)
