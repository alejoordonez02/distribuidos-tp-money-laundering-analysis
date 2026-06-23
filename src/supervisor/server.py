import logging
from queue import Queue
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket
from threading import Thread
from typing import Sequence

from common.comms.messages import (
    Message,
    SupervisorElection,
    SupervisorLeader,
    deserialize_message,
)
from common.comms.messages.message_types import MessageType
from common.comms.transport import Connection

from .event import EventType, LeaderDown, NewLeader, PeerConnection, SupervisorEvent
from .peer import Peer
from .registry import NodeRegistry
from .runtime import LeaderDownError, LeaderRuntime, ReplicaRuntime, SupervisorRuntime
from .tui import Dashboard


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
        dashboard: Dashboard | None = None,
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
        self._internal_port = internal_port
        self._leader_port = leader_port
        self._peers = peers
        self._registry = registry
        self._sweep_interval = sweep_interval
        self._ping_delay = ping_delay
        self._dashboard = dashboard
        self._runtime: SupervisorRuntime | None = None

        self._runtimes: Queue[SupervisorRuntime] = Queue()
        self._events: Queue[SupervisorEvent] = Queue()
        self._events.put(LeaderDown())

        self._runtime_handle = Thread(target=self._runtime_worker)
        self._listener_handle = Thread(target=self._listener_worker)

        self._on_election = False
        self._leader: Peer | None = None
        self._keep_running = False

    def start(self):
        self._keep_running = True

        self._runtime_handle.start()
        self._listener_handle.start()
        self._event_worker()

    def _is_leader(self):
        return not self._leader and self._runtime

    def _broadcast_message(self, msg: Message, peers: Sequence[Peer]) -> int:
        acks = 0
        for p in peers:
            skt = socket(AF_INET, SOCK_STREAM)
            skt.settimeout(0.5)
            try:
                skt.connect((p.host, self._internal_port))
                skt.settimeout(None)
                conn = Connection(skt)
                conn.send(msg.serialize())
                acks += 1
                conn.close()

            except OSError:
                logging.debug(
                    f"could not send {msg.__class__.__name__} message to {p.__dict__}"
                )
                continue

        return acks

    def _event_worker(self):
        def handle_leader_down(_: LeaderDown):
            if self._on_election:
                return
            if self._is_leader():
                self._broadcast_message(SupervisorLeader(self._idx), self._peers)
                return

            self._on_election = True

            greater_peers = [p for p in self._peers if p > self._idx]
            acks = self._broadcast_message(SupervisorElection(), greater_peers)

            if acks > 0:
                self._on_election = False
                return

            self._broadcast_message(SupervisorLeader(self._idx), self._peers)

            if self._runtime:
                self._runtime.stop()
            self._runtimes.put(
                LeaderRuntime(
                    self._server_listener,
                    self._replica_listener,
                    self._registry,
                    self._sweep_interval,
                    self._dashboard,
                )
            )
            self._leader = None
            self._on_election = False

        def handle_new_leader(event: NewLeader):
            leader_idx = event.idx
            if self._leader and leader_idx == self._leader.idx:
                return

            leader_host = next(p.host for p in self._peers if p.idx == leader_idx)

            self._leader = Peer(leader_idx, leader_host)
            if self._runtime:
                self._runtime.stop()
            self._runtimes.put(
                ReplicaRuntime((leader_host, self._leader_port), self._ping_delay)
            )
            self._on_election = False

        def handle_peer_connection(event: PeerConnection):
            conn: Connection = event.conn  # type:ignore [reportAttributeAccessIssue]

            msg = deserialize_message(conn.recv())
            match msg.type():
                case MessageType.SUPERVISOR_ELECTION:
                    self._events.put(LeaderDown())
                case MessageType.SUPERVISOR_LEADER:
                    leader_idx = msg.idx  # type:ignore [reportAttributeAccessIssue]
                    handle_new_leader(NewLeader(leader_idx))

            conn.close()

        while self._keep_running:
            event = self._events.get()
            match event.type():
                case EventType.LEADER_DOWN:
                    handle_leader_down(event)  # type:ignore [reportArgumentType]
                case EventType.PEER_CONNECTION:
                    handle_peer_connection(event)  # type:ignore [reportArgumentType]

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
        if self._runtime:
            self._runtime.stop()
