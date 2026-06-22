import logging
import threading
from socket import AF_INET, SOCK_STREAM, create_connection, socket
from typing import Optional

from common.comms.supervisor import Heartbeat, Register, encode
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
        self._conn: Optional[Connection] = None
        self._thread = threading.Thread(target=self._run, name="heartbeat", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        conn = self._conn
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass
        self._thread.join(timeout=self._interval + 1)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._serve()
            except OSError as e:
                if not self._stop.is_set():
                    logging.warning("heartbeat: supervisor unreachable (%s)", e)
            if not self._stop.is_set():
                self._stop.wait(self._reconnect_delay)

    def _serve(self) -> None:
        skt = socket(AF_INET, SOCK_STREAM)
        try:
            skt = create_connection((self._host, self._port))
            conn = Connection(skt)
            self._conn = conn
            conn.send(encode(Register(self._node_id, self._kind)))
            while not self._stop.is_set():
                conn.send(encode(Heartbeat(self._node_id)))
                self._stop.wait(self._interval)
        finally:
            self._conn = None
            try:
                skt.close()
            except OSError:
                pass
