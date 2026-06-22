import logging
from queue import Queue
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket
from threading import Thread
from typing import Sequence

from common.comms.transport import Connection

from .event import EventType, LeaderDown, PeerConnection, SupervisorEvent
from .peer import Peer
from .registry import NodeRegistry
from .runtime import LeaderDownError, LeaderRuntime, ReplicaRuntime, SupervisorRuntime


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
        self._runtimes: Queue[SupervisorRuntime] = Queue()
        self._events: Queue[SupervisorEvent] = Queue()

        self._runtime: SupervisorRuntime
        runtime = (
            ReplicaRuntime((leader.host, leader_port), ping_delay)
            if leader
            else LeaderRuntime(
                self._server_listener, self._replica_listener, registry, sweep_interval
            )
        )
        self._runtimes.put(runtime)

        self._event_handle = Thread(target=self._event_worker)
        self._runtime_handle = Thread(target=self._runtime_worker)
        self._listener_handle = Thread(target=self._listener_worker)

        self._on_election = False
        self._keep_running = False

    def start(self):
        self._keep_running = True

        self._runtime_handle.start()
        self._listener_handle.start()
        self._event_handle.start()

    def _event_worker(self):
        while self._keep_running:
            event = self._events.get()
            match event.type():
                case EventType.LEADER_DOWN:
                    raise NotImplementedError

    def _runtime_worker(self):
        while self._keep_running:
            runtime = self._runtimes.get()
            if not runtime:
                break

            logging.debug(f"running as {runtime.__class__.__name__}")
            self._runtime = runtime
            try:
                self._runtime.start()
            except LeaderDownError:
                self._events.put(LeaderDown())

    def _listener_worker(self):
        self._node_listener.listen()
        while self._keep_running:
            skt, _ = self._node_listener.accept()
            conn = Connection(skt)
            self._events.put(PeerConnection(conn))

    def stop(self):
        self._keep_running = False
        self._runtime.stop()
