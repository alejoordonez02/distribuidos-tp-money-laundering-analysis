"""Per-client EOF completion over a ring of N peers, as a pure state machine.

No I/O and no threads: the owning controller feeds it events and performs the
actions it returns. Because all of its state lives in one place and is mutated by
the controller's single consume thread, it rides the checkpoint atomically and a
crash restores a consistent phase — idempotency falls out of the phase, not patches.

Model (affinity: each peer owns its input shard and gets its own upstream EOF):
  1. A peer counts the unique messages it received. When it has seen `expected`
     (from its EOF), its input is locally complete -> the controller emits results
     (stateful) and reports how many it sent downstream.
  2. A single barrier token circulates carrying, per peer, (done, sent_count). When
     every peer is done, the leader forwards one downstream EOF with the total sent.

A redelivered token after a crash only re-sets a peer's own slot to the same value
(idempotent), so the barrier can neither double-count nor double-forward.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any
from uuid import UUID


class Phase(Enum):
    PROCESSING = auto()  # still receiving this client's data
    EMITTED = auto()  # local input complete, results emitted, reported to the ring
    DONE = auto()  # barrier complete, downstream EOF forwarded


@dataclass
class _Client:
    expected: int = -1  # set when the upstream EOF arrives
    received: int = 0  # unique messages processed for this client
    sent: int = 0  # messages this node sent downstream
    phase: Phase = Phase.PROCESSING


@dataclass
class BarrierToken:
    """Circulates the ring once collecting each peer's (done, sent)."""

    client_id: UUID
    origin: int  # the leader that started this barrier
    sent_by: dict[int, int] = field(default_factory=dict)  # peer_id -> sent_count


# Actions returned to the controller (it performs the I/O).
@dataclass
class Emit:
    client_id: UUID


@dataclass
class Forward:
    token: BarrierToken


@dataclass
class DownstreamEOF:
    client_id: UUID
    expected: int  # total sent across the cluster


class RingCompletion:
    def __init__(self, node_id: int, peer_ids: list[int]):
        self.node_id = node_id
        self.n_nodes = len(peer_ids) + 1
        self._clients: dict[UUID, _Client] = {}

    def _client(self, client_id: UUID) -> _Client:
        return self._clients.setdefault(client_id, _Client())

    def on_data(self, client_id: UUID):
        self._client(client_id).received += 1

    def on_upstream_eof(self, client_id: UUID, expected: int) -> list[Any]:
        c = self._client(client_id)
        c.expected = expected
        return self._maybe_local_complete(client_id)

    def _maybe_local_complete(self, client_id: UUID) -> list[Any]:
        c = self._client(client_id)
        if c.phase != Phase.PROCESSING or c.expected < 0 or c.received < c.expected:
            return []
        # input fully received: tell the controller to emit, then await report_sent
        return [Emit(client_id)]

    def report_sent(self, client_id: UUID, sent: int) -> list[Any]:
        """Called by the controller right after it emits (stateful) or finishes its
        per-message output (stateless), with this node's total sent for the client."""
        c = self._client(client_id)
        c.sent = sent
        c.phase = Phase.EMITTED
        token = BarrierToken(client_id, origin=self.node_id, sent_by={self.node_id: sent})
        return self._advance(token)

    def on_token(self, token: BarrierToken) -> list[Any]:
        c = self._client(token.client_id)
        # idempotent: re-setting our own slot to the same value changes nothing
        token.sent_by[self.node_id] = c.sent
        return self._advance(token)

    def _advance(self, token: BarrierToken) -> list[Any]:
        if len(token.sent_by) < self.n_nodes:
            return [Forward(token)]
        # every peer reported -> the leader closes the barrier exactly once
        if token.origin != self.node_id:
            return [Forward(token)]
        c = self._client(token.client_id)
        if c.phase == Phase.DONE:
            return []
        c.phase = Phase.DONE
        return [DownstreamEOF(token.client_id, expected=sum(token.sent_by.values()))]

    def snapshot_state(self) -> dict[str, Any]:
        return {
            str(cid): [c.expected, c.received, c.sent, c.phase.name]
            for cid, c in self._clients.items()
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self._clients = {}
        for cid, (expected, received, sent, phase) in snapshot.items():
            self._clients[UUID(cid)] = _Client(expected, received, sent, Phase[phase])
