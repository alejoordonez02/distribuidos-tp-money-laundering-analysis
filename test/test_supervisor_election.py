"""Unit tests for the supervisor leader-election state machine.

Covers the Bully + split-brain reconciliation logic that lives in
``SupervisorNode._handle_new_leader`` and ``SupervisorNode._handle_leader_down``.

The node is built WITHOUT starting any threads (``start()`` is never called).
``_make_skt`` is patched so no real socket is bound, and the side-effecting
methods ``_broadcast_message`` / ``_promote`` / ``_downgrade`` are replaced by
mocks so we can assert *who* the node re-affirms to, elects, promotes or
downgrades against, in pure isolation.
"""

from unittest.mock import MagicMock, patch

import pytest

from common.comms.messages import SupervisorElection, SupervisorLeader
from supervisor.event import LeaderDown, NewLeader
from supervisor.peer import Peer
from supervisor.server import SupervisorNode


def _make_node(idx, peers, broadcast_returns=0):
    """Build a SupervisorNode without binding sockets or starting threads.

    The election handlers only touch ``self`` state and the three mocked
    side-effecting methods, so this is enough to drive them directly.
    """
    with patch("supervisor.server._make_skt", return_value=MagicMock()):
        node = SupervisorNode(
            idx=idx,
            bind_host="localhost",
            server_port=0,
            internal_port=0,
            leader_port=0,
            peers=peers,
            registry_factory=MagicMock(),
            reviver_factory=MagicMock(),
            dashboard_factory=MagicMock(),
        )
    node._broadcast_message = MagicMock(return_value=broadcast_returns)
    node._promote = MagicMock()
    node._downgrade = MagicMock()
    return node


def _broadcast_calls(node):
    """List of (msg, peers) tuples passed to the mocked _broadcast_message."""
    return [(c.args[0], c.args[1]) for c in node._broadcast_message.call_args_list]


# --------------------------------------------------------------------------- #
# _handle_new_leader
# --------------------------------------------------------------------------- #


def test_adopts_higher_leader():
    # idx=2, a peer with id 5 claims leadership -> adopt it and downgrade to replica
    node = _make_node(2, [Peer(5, "host5"), Peer(3, "host3")])

    node._handle_new_leader(NewLeader(5))

    node._downgrade.assert_called_once_with("host5")
    assert node._leader == Peer(5, "host5")
    assert node._on_election is False
    node._promote.assert_not_called()


def test_ignores_lower_claim_when_not_leader():
    # idx=5, a peer with id 2 claims leadership and I am NOT leader -> do nothing
    node = _make_node(5, [Peer(2, "host2")])
    node._runtime = None  # not a leader
    node._leader = None

    node._handle_new_leader(NewLeader(2))

    node._broadcast_message.assert_not_called()
    node._downgrade.assert_not_called()
    node._promote.assert_not_called()
    assert node._leader is None


def test_reasserts_on_lower_claim_when_leader():
    # idx=5 and I AM leader; a peer with id 2 claims leadership ->
    # re-broadcast SupervisorLeader(5) so the impostor steps down (split-brain fix)
    peers = [Peer(2, "host2"), Peer(3, "host3")]
    node = _make_node(5, peers)
    node._runtime = MagicMock()  # leader: _runtime set and _leader is None
    node._leader = None

    node._handle_new_leader(NewLeader(2))

    calls = _broadcast_calls(node)
    assert len(calls) == 1
    msg, sent_peers = calls[0]
    assert isinstance(msg, SupervisorLeader)
    assert msg.idx == 5
    assert sent_peers == peers
    node._downgrade.assert_not_called()


def test_does_not_downgrade_to_leader_lower_than_current():
    # idx=2 already following leader 5; a claim of 4 (>my idx, but < current leader)
    # must NOT cause a downgrade -- never follow a lower leader than the current one
    node = _make_node(2, [Peer(4, "host4"), Peer(5, "host5")])
    node._leader = Peer(5, "host5")

    node._handle_new_leader(NewLeader(4))

    node._downgrade.assert_not_called()
    node._broadcast_message.assert_not_called()
    assert node._leader == Peer(5, "host5")


# --------------------------------------------------------------------------- #
# _handle_leader_down
# --------------------------------------------------------------------------- #


def test_election_with_acks_cancels_promotion():
    # idx=2, greater peer exists and ACKs the election -> someone bigger is alive,
    # so I stand down: no promotion, on_election cleared
    node = _make_node(2, [Peer(5, "host5")], broadcast_returns=1)
    node._runtime = None  # not leader
    node._leader = None

    node._handle_leader_down(LeaderDown())

    node._promote.assert_not_called()
    assert node._on_election is False
    # exactly one broadcast: the SupervisorElection to greater peers
    calls = _broadcast_calls(node)
    assert len(calls) == 1
    msg, sent_peers = calls[0]
    assert isinstance(msg, SupervisorElection)
    assert sent_peers == [Peer(5, "host5")]


def test_promotes_when_no_acks():
    # idx=5, no greater peer answers -> proclaim myself leader and promote
    node = _make_node(5, [Peer(2, "host2"), Peer(3, "host3")], broadcast_returns=0)
    node._runtime = None  # not leader
    node._leader = None

    node._handle_leader_down(LeaderDown())

    node._promote.assert_called_once()
    assert node._leader is None
    assert node._on_election is False

    calls = _broadcast_calls(node)
    # first an election to greater peers (none here), then SupervisorLeader to all
    msgs = [type(m) for m, _ in calls]
    assert SupervisorElection in msgs
    assert SupervisorLeader in msgs
    # the leader announcement claims my own idx and goes to all peers
    leader_call = next((m, p) for m, p in calls if isinstance(m, SupervisorLeader))
    assert leader_call[0].idx == 5
    assert leader_call[1] == [Peer(2, "host2"), Peer(3, "host3")]


def test_leader_reaffirms_on_leader_down():
    # if I am already leader and get a LeaderDown, I just re-assert, never re-elect
    node = _make_node(7, [Peer(2, "host2")])
    node._runtime = MagicMock()  # leader
    node._leader = None

    node._handle_leader_down(LeaderDown())

    node._promote.assert_not_called()
    calls = _broadcast_calls(node)
    assert len(calls) == 1
    msg, _ = calls[0]
    assert isinstance(msg, SupervisorLeader)
    assert msg.idx == 7


def test_leader_down_ignored_during_election():
    # already running an election -> a new LeaderDown is a no-op
    node = _make_node(2, [Peer(5, "host5")])
    node._on_election = True

    node._handle_leader_down(LeaderDown())

    node._broadcast_message.assert_not_called()
    node._promote.assert_not_called()
    assert node._on_election is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
