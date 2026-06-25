"""Per-client EOF completion over a ring of N peers, as a pure state machine.

No I/O and no threads: the owning controller feeds it events and performs the
actions it returns. Because all of its state lives in one place and is mutated by
the controller's single consume thread, it rides the checkpoint atomically and a
crash restores a consistent phase — idempotency falls out of the phase, not patches.

Model (affinity: each peer owns its input shard and gets its own upstream EOF):
  1. A peer counts the unique messages it received. When it has seen `expected`
     (from its EOF), its input is locally complete -> the controller emits results
     (stateful) and reports how many it sent to each downstream shard.
  2. A single barrier token circulates carrying, per peer, its per-shard sent counts.
     When every peer is done, the leader forwards one downstream EOF per shard, each
     with that shard's total across the cluster (a single downstream is just shard 0).

A redelivered token after a crash only re-sets a peer's own slot to the same value
(idempotent), so the barrier can neither double-count nor double-forward.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any
from uuid import UUID

CLIENTS_KEY = "clients"
ABORTED_KEY = "aborted"


class Phase(Enum):
    PROCESSING = auto()  # still receiving this client's data
    EMITTED = auto()  # local input complete, results emitted, reported to the ring
    DONE = auto()  # barrier complete, downstream EOF forwarded


@dataclass
class _Client:
    expected: int = -1  # set when the upstream EOF arrives
    received: int = 0  # unique messages processed for this client
    sent: dict[int, int] = field(default_factory=dict)  # downstream shard -> count
    phase: Phase = Phase.PROCESSING


@dataclass
class BarrierToken:
    """Circulates the ring collecting each peer's per-shard sent counts."""

    client_id: UUID
    origin: int  # the leader that started this barrier
    # peer_id -> {downstream shard -> count}
    sent_by: dict[int, dict[int, int]] = field(default_factory=dict)


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
    expected_per_shard: dict[int, int]  # downstream shard -> total sent across cluster


class RingCompletion:
    def __init__(self, node_id: int, peer_ids: list[int]):
        self.node_id = node_id
        self.n_nodes = len(peer_ids) + 1
        # one fixed leader circulates and closes the single barrier token; redundant
        # tokens would only force every downstream consumer to dedup re-emitted EOFs.
        self.leader = min([node_id, *peer_ids])
        self._clients: dict[UUID, _Client] = {}
        # tombstone aborted clients: a barrier token still circulating would otherwise
        # re-create the client on each hop and loop forever — on_token drops it instead.
        self._aborted: set[UUID] = set()

    def _client(self, client_id: UUID) -> _Client:
        return self._clients.setdefault(client_id, _Client())

    def on_data(self, client_id: UUID) -> list[Any]:
        if client_id in self._aborted:
            return []
        self._client(client_id).received += 1
        return self._maybe_local_complete(client_id)

    def drop(self, client_id: UUID):
        """Forget and tombstone a client's completion state when it aborts, so its
        partial counts never complete and any in-flight barrier token for it dies
        instead of resurrecting the client and circulating forever."""
        self._clients.pop(client_id, None)
        self._aborted.add(client_id)

    def on_upstream_eof(self, client_id: UUID, expected: int) -> list[Any]:
        if client_id in self._aborted:
            return []
        c = self._client(client_id)
        c.expected = expected
        return self._maybe_local_complete(client_id)

    def _maybe_local_complete(self, client_id: UUID) -> list[Any]:
        c = self._client(client_id)
        if c.phase != Phase.PROCESSING or c.expected < 0 or c.received < c.expected:
            return []
        return [Emit(client_id)]

    def recheck(self) -> list[Any]:
        actions: list[Any] = []
        for client_id in list(self._clients):
            actions.extend(self._maybe_local_complete(client_id))
        return actions

    def resolved_clients(self) -> list[UUID]:
        """Clients whose result was already emitted (EMITTED) or whose barrier closed
        (DONE). On restore their spilled state is safe to free: it will never be
        re-emitted, so a revived node must drop it instead of orphaning it on disk."""
        return [
            cid for cid, c in self._clients.items() if c.phase != Phase.PROCESSING
        ]

    def report_sent(self, client_id: UUID, sent: dict[int, int]) -> list[Any]:
        """Called by the controller right after it emits (stateful) or finishes its
        per-message output (stateless), with this node's per-shard sent counts."""
        c = self._client(client_id)
        if c.phase != Phase.PROCESSING:  # idempotent on EOF redelivery
            return []
        c.sent = dict(sent)
        c.phase = Phase.EMITTED
        if self.node_id != self.leader:
            # non-leaders just wait to be collected by the leader's token
            return []
        token = BarrierToken(
            client_id, origin=self.leader, sent_by={self.node_id: c.sent}
        )
        return self._advance(token)

    def on_token(self, token: BarrierToken) -> list[Any]:
        if token.client_id in self._aborted:
            return []  # the client aborted: drop its circulating token, don't forward
        c = self._client(token.client_id)
        # only count a peer that has already emitted; a token passing a peer still
        # PROCESSING must not record its stale (zero) slot. idempotent on redelivery.
        if c.phase != Phase.PROCESSING:
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
        per_shard: dict[int, int] = {}
        for shard_counts in token.sent_by.values():
            for shard, count in shard_counts.items():
                per_shard[shard] = per_shard.get(shard, 0) + count
        return [DownstreamEOF(token.client_id, expected_per_shard=per_shard)]

    def snapshot_state(self) -> dict[str, Any]:
        return {
            CLIENTS_KEY: {
                str(cid): [
                    c.expected,
                    c.received,
                    {str(s): n for s, n in c.sent.items()},
                    c.phase.name,
                ]
                for cid, c in self._clients.items()
            },
            ABORTED_KEY: [str(cid) for cid in self._aborted],
        }

    def restore_state(self, snapshot: dict[str, Any]):
        # tolerate the pre-tombstone flat format (just a clients map) for old checkpoints
        clients = snapshot[CLIENTS_KEY] if CLIENTS_KEY in snapshot else snapshot
        self._clients = {}
        for cid, (expected, received, sent, phase) in clients.items():
            self._clients[UUID(cid)] = _Client(
                expected, received, {int(s): n for s, n in sent.items()}, Phase[phase]
            )
        self._aborted = {UUID(cid) for cid in snapshot.get(ABORTED_KEY, [])}
