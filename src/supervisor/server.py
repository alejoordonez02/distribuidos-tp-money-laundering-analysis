import logging
import threading
import time
from abc import ABC
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket
from typing import Sequence

from common.comms.supervisor import Heartbeat, Register, decode
from common.comms.transport import Connection

from .registry import NodeRegistry


class SupervisorRuntime(ABC): ...


class LeaderRuntime(SupervisorRuntime):
    def __init__(
        self,
        server_listener: socket,
        replica_listener: socket,
        registry: NodeRegistry,
        sweep_interval: float = 0.5,
    ):
        self._server_listener = server_listener
        self._replica_listener = replica_listener
        self._registry = registry
        self._sweep_interval = sweep_interval

        self._stop = threading.Event()

    def start(self):
        self._server_listener.listen()
        threading.Thread(target=self._sweep, name="sweeper", daemon=True).start()
        threading.Thread(target=self._accept_loop, name="accept", daemon=True).start()

    def stop(self):
        self._stop.set()
        try:
            self._server_listener.close()
        except OSError:
            pass

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn_skt, _ = self._server_listener.accept()
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


class SupervisorNode:
    def __init__(
        self,
        bind_host: str,
        server_port: int,
        internal_port: int,
        leader_port: int,
        peer_addrs: Sequence[tuple[str, int]],
        registry: NodeRegistry,
        sweep_interval: float = 0.5,
    ):
        def make_skt(addr: tuple[str, int]):
            skt = socket(AF_INET, SOCK_STREAM)
            skt.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            skt.bind(addr)
            return skt

        addrs = [
            (bind_host, server_port),
            (bind_host, leader_port),
            (bind_host, internal_port),
        ]

        self._server_listener, self._replica_listener, self._node_listener = [
            make_skt(addr) for addr in addrs
        ]
        self._leader_port = leader_port
        self._peer_addrs = peer_addrs
        self._registry = registry
        self._sweep_interval = sweep_interval

        self._runtime = LeaderRuntime(
            self._server_listener, self._replica_listener, registry, sweep_interval
        )

    def start(self):
        self._runtime.start()

    def stop(self):
        self._runtime.stop()
