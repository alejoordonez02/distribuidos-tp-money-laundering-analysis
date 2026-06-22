from dataclasses import dataclass
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket
from typing import Self, Sequence

from .registry import NodeRegistry
from .runtime import LeaderRuntime, ReplicaRuntime, SupervisorRuntime


@dataclass
class Peer:
    idx: int
    host: str

    def __gt__(self, other: Self | int) -> bool:
        if self.idx is None or (isinstance(other, Peer) and other.idx is None):
            raise RuntimeError("cannot compare peer without an assigned idx")
        return self.idx > other.idx if isinstance(other, Peer) else self.idx > other  # type: ignore[reportOperatorIssue]

    def __lt__(self, other: Self | int) -> bool:
        if self.idx is None or (isinstance(other, Peer) and not other.idx):
            raise RuntimeError("cannot compare peer without an assigned idx")
        return self.idx < other.idx if isinstance(other, Peer) else self.idx < other  # type: ignore[reportOperatorIssue]


class SupervisorNode:
    def __init__(
        self,
        idx: int,
        bind_host: str,
        server_port: int,
        internal_port: int,
        leader_port: int,
        peers: Sequence[Peer],
        registry: NodeRegistry,
        sweep_interval: float = 0.5,
        ping_delay: float = 0.5,
    ):
        def make_skt(addr: tuple[str, int]):
            skt = socket(AF_INET, SOCK_STREAM)
            skt.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            skt.bind(addr)
            return skt

        self._idx = idx

        addrs = [
            (bind_host, server_port),
            (bind_host, leader_port),
            (bind_host, internal_port),
        ]

        self._server_listener, self._replica_listener, self._node_listener = [
            make_skt(addr) for addr in addrs
        ]
        self._leader_port = leader_port
        self._peers = peers
        self._registry = registry
        self._sweep_interval = sweep_interval

        # TODO: esto se elige dinámicamente pero ahora sólo quiero ping pong
        leader = max(peers) if len(peers) > 0 and idx < max(peers) else None
        self._runtime: SupervisorRuntime = (
            ReplicaRuntime((leader.host, leader_port), ping_delay)
            if leader
            else LeaderRuntime(
                self._server_listener, self._replica_listener, registry, sweep_interval
            )
        )

    def start(self):
        self._runtime.start()

    def stop(self):
        self._runtime.stop()
