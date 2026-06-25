import logging
import time
from socket import SHUT_RDWR, socket
from threading import Event, Thread

from common.comms.supervisor import Heartbeat, Register, decode, encode
from common.comms.transport import Connection

from ..make_skt import _make_skt
from ..registry import NodeRegistry
from ..reviver import Reviver
from ..tui import Dashboard
from .supervisor_runtime import SupervisorRuntime


class ReplicaDownError(Exception): ...


def _shutdown_skt(skt: socket):
    try:
        skt.shutdown(SHUT_RDWR)
    except OSError:
        pass
    try:
        skt.close()
    except OSError:
        pass


class ReplicaLink:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._keep_running = True

    def pong_ping(self):
        try:
            while self._keep_running:
                ping = self._conn.recv()
                if not ping:
                    raise ReplicaDownError()

                self._conn.send(b"pong")

        except OSError as e:
            logging.debug("lost connection with replica (%s)", e)
        finally:
            try:
                self._conn.close()
            except OSError:
                pass

    def close(self):
        try:
            self._conn.close()
        except OSError:
            pass


class LeaderRuntime(SupervisorRuntime):
    def __init__(
        self,
        server_bind: tuple[str, int],
        leader_bind: tuple[str, int],
        registry: NodeRegistry,
        reviver: Reviver | None,
        dashboard: Dashboard | None = None,
        sweep_interval: float = 0.5,
    ):
        self._server_bind = server_bind
        self._leader_bind = leader_bind

        self._server_listener: socket | None = None
        self._replica_listener: socket | None = None

        self._registry = registry
        self._reviver = reviver
        self._dashboard = dashboard
        self._sweep_interval = sweep_interval

        self._stop = Event()

        self._sweeper_handle = Thread(target=self._sweep, name="sweeper", daemon=True)
        self._reviver_handle = (
            Thread(target=self._reviver.run, args=(self._stop,))
            if self._reviver
            else None
        )
        self._accept_handle = Thread(
            target=self._handle_clients, name="accept", daemon=True
        )
        self._replicas_handle = Thread(
            target=self._handle_replicas, name="replicas", daemon=True
        )
        self._dashboard_handle = (
            Thread(
                target=self._dashboard.run,
                args=(self._stop,),
                name="dashboard",
                daemon=True,
            )
            if self._dashboard
            else None
        )

    def start(self):
        if self._stop.is_set() or self._server_listener or self._replica_listener:
            return

        self._server_listener = _make_skt(self._server_bind)
        self._replica_listener = _make_skt(self._leader_bind)

        self._sweeper_handle.start()
        self._accept_handle.start()
        self._replicas_handle.start()
        if self._reviver_handle:
            self._reviver_handle.start()
        if self._dashboard_handle:
            self._dashboard_handle.start()

    def stop(self):
        self._stop.set()
        if self._server_listener:
            _shutdown_skt(self._server_listener)
        if self._replica_listener:
            _shutdown_skt(self._replica_listener)

        self._sweeper_handle.join()
        self._accept_handle.join()
        self._replicas_handle.join()
        if self._reviver_handle:
            self._reviver_handle.join()
        if self._dashboard_handle:
            self._dashboard_handle.join()

    def _handle_clients(self):
        def handle_client(conn_skt: socket):
            conn = Connection(conn_skt)
            try:
                while not self._stop.is_set():
                    data = conn.recv()
                    if not data:
                        break

                    now = time.monotonic()
                    msg = decode(data)
                    if isinstance(msg, Register):
                        self._registry.register(msg.node_id, msg.kind, now)
                    elif isinstance(msg, Heartbeat):
                        self._registry.heartbeat(msg.node_id, now)
                    conn.send(encode(Heartbeat("supervisor")))

            except (OSError, ValueError) as e:
                logging.debug("supervisor: connection dropped (%s)", e)
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

        assert self._server_listener  # pleasing linter
        self._server_listener.listen()
        while not self._stop.is_set():
            try:
                conn_skt, _ = self._server_listener.accept()
            except OSError:
                break
            Thread(target=handle_client, args=(conn_skt,), daemon=True).start()

    def _handle_replicas(self):
        def handle_replica(conn_skt):
            rep = ReplicaLink(Connection(conn_skt))
            try:
                while not self._stop.is_set():
                    rep.pong_ping()
            except ReplicaDownError as e:
                logging.debug("lost connection with replica (%s)", e)

        assert self._replica_listener  # pleasing linter
        self._replica_listener.listen()
        while not self._stop.is_set():
            try:
                conn_skt, _ = self._replica_listener.accept()
            except OSError:
                break
            Thread(target=handle_replica, args=(conn_skt,), daemon=True).start()

    def _sweep(self) -> None:
        while not self._stop.is_set():
            self._registry.check_timeouts(time.monotonic())
            self._stop.wait(self._sweep_interval)
