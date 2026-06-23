import logging
import threading
import time
from socket import socket

from common.comms.supervisor import Heartbeat, Register, decode, encode
from common.comms.transport import Connection

from ..registry import NodeRegistry
from ..tui import Dashboard
from .supervisor_runtime import SupervisorRuntime


class ReplicaDownError(Exception): ...


class Replica:
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
        server_listener: socket,
        replica_listener: socket,
        registry: NodeRegistry,
        sweep_interval: float = 0.5,
        dashboard: Dashboard | None = None,
    ):
        self._server_listener = server_listener
        self._replica_listener = replica_listener
        self._registry = registry
        self._sweep_interval = sweep_interval
        self._dashboard = dashboard

        self._stop = threading.Event()

    def start(self):
        threading.Thread(target=self._sweep, name="sweeper", daemon=True).start()
        threading.Thread(
            target=self._handle_clients, name="accept", daemon=True
        ).start()
        threading.Thread(
            target=self._handle_replicas, name="replicas", daemon=True
        ).start()
        if self._dashboard:
            threading.Thread(
                target=self._dashboard.run,
                args=(self._stop,),
                name="dashboard",
                daemon=True,
            ).start()
        # TODO: mepa q está como el orto usar daemon, no tiene mucho sentido y
        #       aparte cuándo se joinean los threads? no importa en qué terminan?

    def stop(self):
        self._stop.set()
        try:
            self._server_listener.close()
        except OSError:
            pass

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

        self._server_listener.listen()
        while not self._stop.is_set():
            try:
                conn_skt, _ = self._server_listener.accept()
            except OSError:
                break
            threading.Thread(
                target=handle_client, args=(conn_skt,), daemon=True
            ).start()

    def _handle_replicas(self):
        def handle_replica(conn_skt):
            rep = Replica(Connection(conn_skt))
            try:
                while not self._stop.is_set():
                    rep.pong_ping()  # TODO: creo q tiene más sentido q acá esté el sleep
            except ReplicaDownError as e:
                logging.debug("lost connection with replica (%s)", e)

        self._replica_listener.listen()
        while not self._stop.is_set():
            try:
                conn_skt, _ = self._replica_listener.accept()
            except OSError:
                break
            threading.Thread(
                target=handle_replica, args=(conn_skt,), daemon=True
            ).start()

    def _sweep(self) -> None:
        while not self._stop.is_set():
            self._registry.check_timeouts(time.monotonic())
            self._stop.wait(self._sweep_interval)
