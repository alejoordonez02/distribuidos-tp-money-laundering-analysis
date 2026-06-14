from typing import Callable

from .factory import make_heartbeat_client


def run_with_heartbeat(start: Callable[[], None]) -> None:
    """Run a node's blocking start() while a heartbeat client reports liveness to
    the supervisor. A no-op wrapper when no supervisor is configured, so the
    pipeline runs exactly as before without one."""
    hb = make_heartbeat_client()
    if hb is not None:
        hb.start()
    try:
        start()
    finally:
        if hb is not None:
            hb.stop()
