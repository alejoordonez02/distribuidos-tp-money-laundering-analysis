import logging
import threading
from socket import create_connection

from common.comms.supervisor import Heartbeat, Register, decode, encode
from common.comms.transport import Connection


class HeartbeatClient:
    """Background TCP client that registers a node with the supervisor and
    sends periodic heartbeats. It runs on a daemon thread so it never blocks the
    node's work, and reconnects on its own if the supervisor is down or restarts."""

    def __init__(
        self,
        node_id: str,
        kind: str,
        host: str,
        port: int,
        interval: float = 2.0,
        reconnect_delay: float = 2.0,
    ):
        self._node_id = node_id
        self._kind = kind
        self._host = host
        self._port = port
        self._interval = interval
        self._reconnect_delay = reconnect_delay
        self._stop = threading.Event()
        self._conn: Connection | None = None
        self._thread = threading.Thread(target=self._run, name="heartbeat", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _connect_to_server(self):
        while not self._stop.is_set():
            self._stop.wait(self._reconnect_delay)

            try:
                skt = create_connection(
                    (self._host, self._port), timeout=self._interval
                )
                conn = Connection(skt)
                conn.send(encode(Register(self._node_id, self._kind)))

                self._conn = conn
                return
            except OSError as e:
                logging.warning("supervisor unreachable, reconnecting (%s)", e)

    def _run(self) -> None:
        self._connect_to_server()

        while not self._stop.is_set():
            try:
                self._heartbeat()
            except OSError as e:
                if self._stop.is_set():
                    break

                logging.warning("supervisor unreachable, reconnecting (%s)", e)
                self._connect_to_server()

    def _heartbeat(self) -> None:
        if not self._conn:
            raise RuntimeError("cannot hearbeat without a initialized conneciton")
        self._conn.send(encode(Heartbeat(self._node_id)))
        msg = decode(self._conn.recv())

        if not isinstance(msg, Heartbeat):
            raise OSError("lost connection with supervisor")

        self._stop.wait(self._interval)

    def stop(self) -> None:
        self._stop.set()
        conn = self._conn
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass
        self._thread.join(timeout=self._interval + 1)
