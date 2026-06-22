from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket
from typing import Sequence

from .registry import NodeRegistry
from .runtime import LeaderRuntime


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
