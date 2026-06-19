import logging
import threading
import time
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket

from common.comms.supervisor import Heartbeat, Register, decode
from common.comms.transport import Connection

from .registry import NodeRegistry


class SupervisorServer:
    """TCP server that accepts node connections and feeds their Register and
    Heartbeat messages into the registry. Liveness is decided by the registry's
    timeout sweep — never by Docker and never by the socket state."""

    def __init__(
        self,
        host: str,
        port: int,
        registry: NodeRegistry,
        sweep_interval: float = 0.5,
    ):
        self._host = host
        self._port = port
        self._registry = registry
        self._sweep_interval = sweep_interval
        self._stop = threading.Event()
        self._skt = socket(AF_INET, SOCK_STREAM)

        self._skt.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self._skt.bind((self._host, self._port))

    def start(self) -> None:
        self._skt.listen()
        threading.Thread(target=self._sweep, name="sweeper", daemon=True).start()
        threading.Thread(target=self._accept_loop, name="accept", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._skt.close()
        except OSError:
            pass

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn_skt, _ = self._skt.accept()
            except OSError:
                break
            threading.Thread(target=self._handle, args=(conn_skt,), daemon=True).start()

    def _handle(self, conn_skt: socket) -> None:
        conn = Connection(conn_skt)
        try:
            while not self._stop.is_set():
                data = conn.recv()
                if not data:
                    break
                self._dispatch(decode(data))
        except (OSError, ValueError) as e:
            logging.debug("supervisor: connection dropped (%s)", e)
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _dispatch(self, msg) -> None:
        now = time.monotonic()
        if isinstance(msg, Register):
            self._registry.register(msg.node_id, msg.kind, now)
        elif isinstance(msg, Heartbeat):
            self._registry.heartbeat(msg.node_id, now)

    def _sweep(self) -> None:
        while not self._stop.is_set():
            self._registry.check_timeouts(time.monotonic())
            self._stop.wait(self._sweep_interval)
