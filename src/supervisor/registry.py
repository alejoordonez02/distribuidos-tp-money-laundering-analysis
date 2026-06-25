import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Status(str, Enum):
    UNKNOWN = "unknown"
    ALIVE = "alive"
    DEAD = "dead"


@dataclass
class NodeState:
    node_id: str
    kind: str = "node"
    status: Status = Status.UNKNOWN
    last_heartbeat: Optional[float] = None  # monotonic clock


@dataclass
class Event:
    at: float  # wall clock, for display
    node_id: str
    message: str


class NodeRegistry:
    """Thread-safe view of every node's liveness, driven purely by heartbeats.
    A timeout sweep (not Docker, not the socket state) decides when a node is
    considered dead. The monotonic `now` is injected so the sweep is testable."""

    def __init__(
        self,
        timeout: float,
        expected: Optional[list[str]] = None,
        max_events: int = 200,
    ):
        self._timeout = timeout
        self._max_events = max_events
        self._lock = threading.Lock()
        self._nodes: dict[str, NodeState] = {
            node_id: NodeState(node_id) for node_id in (expected or [])
        }
        self._events: list[Event] = []
        # First sweep clock, to give expected-but-never-seen nodes a grace before
        # declaring them dead (so a freshly-elected leader does not revive nodes
        # that are simply still booting / re-registering).
        self._started_at: Optional[float] = None

    def register(self, node_id: str, kind: str, now: float) -> None:
        with self._lock:
            node = self._nodes.setdefault(node_id, NodeState(node_id))
            node.kind = kind
            node.last_heartbeat = now
            self._transition(node, Status.ALIVE, "registered")

    def heartbeat(self, node_id: str, now: float) -> None:
        with self._lock:
            node = self._nodes.setdefault(node_id, NodeState(node_id))
            node.last_heartbeat = now
            if node.status is not Status.ALIVE:
                self._transition(node, Status.ALIVE, "recovered")

    def check_timeouts(self, now: float) -> None:
        with self._lock:
            if self._started_at is None:
                self._started_at = now
            for node in self._nodes.values():
                if (
                    node.status is Status.ALIVE
                    and node.last_heartbeat is not None
                    and now - node.last_heartbeat > self._timeout
                ):
                    self._transition(node, Status.DEAD, "heartbeat lost")
                elif (
                    node.status is Status.UNKNOWN
                    and node.last_heartbeat is None
                    and now - self._started_at > self._timeout
                ):
                    # Expected node that never reported, even after the grace: a
                    # leader elected after a crash must revive it, not stay blind
                    # to it just because it never sent THIS leader a heartbeat.
                    self._transition(node, Status.DEAD, "never seen")

    def note(self, node_id: str, message: str) -> None:
        """Append an external event (e.g. the reviver issuing a docker start) to
        the event log so it shows up in the dashboard."""
        with self._lock:
            self._events.append(Event(time.time(), node_id, message))
            if len(self._events) > self._max_events:
                self._events.pop(0)

    def snapshot(self) -> tuple[list[NodeState], list[Event]]:
        with self._lock:
            nodes = [
                NodeState(n.node_id, n.kind, n.status, n.last_heartbeat)
                for n in self._nodes.values()
            ]
            return nodes, list(self._events)

    def _transition(self, node: NodeState, status: Status, reason: str) -> None:
        if node.status is not status:
            node.status = status
            self._events.append(
                Event(time.time(), node.node_id, f"{reason} -> {status.value}")
            )
            if len(self._events) > self._max_events:
                self._events.pop(0)
