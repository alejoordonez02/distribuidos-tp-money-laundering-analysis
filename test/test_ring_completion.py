from uuid import UUID

from common.comms.eof_handler.ring_completion import (
    BarrierToken,
    DownstreamEOF,
    Emit,
    Forward,
    Phase,
    RingCompletion,
)

C = UUID(int=1)


def _drive_local_complete(rc, node_id, received, expected, sent):
    for _ in range(received):
        rc.on_data(C)
    actions = rc.on_upstream_eof(C, expected)
    assert actions == [Emit(C)]
    return rc.report_sent(C, sent)


def test_single_node_closes_immediately():
    rc = RingCompletion(node_id=0, peer_ids=[])
    actions = _drive_local_complete(rc, 0, received=10, expected=10, sent=4)
    assert actions == [DownstreamEOF(C, expected=4)]


def test_eof_before_all_received_does_not_complete():
    rc = RingCompletion(node_id=0, peer_ids=[])
    rc.on_data(C)
    assert rc.on_upstream_eof(C, expected=5) == []  # only 1 of 5 received
    for _ in range(4):
        rc.on_data(C)
    # the controller re-checks after more data; emulate by re-sending eof state
    assert rc.on_upstream_eof(C, expected=5) == [Emit(C)]


def test_three_nodes_barrier_collects_all_then_leader_closes():
    leader = RingCompletion(node_id=0, peer_ids=[1, 2])
    # leader finishes its shard and starts the barrier
    [fwd] = _drive_local_complete(leader, 0, received=6, expected=6, sent=2)
    assert isinstance(fwd, Forward)
    token = fwd.token
    assert token.sent_by == {0: 2}

    # peer 1 adds its slot, still incomplete -> forwards
    peer1 = RingCompletion(node_id=1, peer_ids=[0, 2])
    _drive_local_complete(peer1, 1, received=6, expected=6, sent=3)
    [fwd1] = peer1.on_token(token)
    assert isinstance(fwd1, Forward)
    assert fwd1.token.sent_by == {0: 2, 1: 3}

    # peer 2 adds its slot -> all present, but peer2 is not the leader -> forwards
    peer2 = RingCompletion(node_id=2, peer_ids=[0, 1])
    _drive_local_complete(peer2, 2, received=6, expected=6, sent=5)
    [fwd2] = peer2.on_token(fwd1.token)
    assert isinstance(fwd2, Forward)

    # back at the leader with all three slots -> closes once with the total
    [done] = leader.on_token(fwd2.token)
    assert done == DownstreamEOF(C, expected=2 + 3 + 5)


def test_redelivered_token_is_idempotent_no_double_close():
    leader = RingCompletion(node_id=0, peer_ids=[1])
    _drive_local_complete(leader, 0, received=4, expected=4, sent=2)
    full = BarrierToken(C, origin=0, sent_by={0: 2, 1: 7})
    assert leader.on_token(full) == [DownstreamEOF(C, expected=9)]
    # the same token redelivered after a crash must NOT close again
    assert leader.on_token(full) == []


def test_snapshot_restore_preserves_phase_after_crash():
    rc = RingCompletion(node_id=0, peer_ids=[1])
    _drive_local_complete(rc, 0, received=4, expected=4, sent=2)  # phase EMITTED
    snap = rc.snapshot_state()

    restored = RingCompletion(node_id=0, peer_ids=[1])
    restored.restore_state(snap)
    # a redelivered upstream EOF after restart must not re-emit (already EMITTED)
    assert restored.on_upstream_eof(C, expected=4) == []
    # the barrier still closes when the full token arrives
    full = BarrierToken(C, origin=0, sent_by={0: 2, 1: 1})
    assert restored.on_token(full) == [DownstreamEOF(C, expected=3)]


def test_peer_that_sent_zero_still_participates():
    leader = RingCompletion(node_id=0, peer_ids=[1])
    _drive_local_complete(leader, 0, received=3, expected=3, sent=0)
    [done] = leader.on_token(BarrierToken(C, origin=0, sent_by={0: 0, 1: 4}))
    assert done == DownstreamEOF(C, expected=4)
