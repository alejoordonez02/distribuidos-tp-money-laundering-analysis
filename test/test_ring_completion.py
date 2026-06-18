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
    # sent is a dict {downstream shard -> count}
    for _ in range(received):
        rc.on_data(C)
    actions = rc.on_upstream_eof(C, expected)
    assert actions == [Emit(C)]
    return rc.report_sent(C, sent)


def test_single_node_closes_immediately():
    rc = RingCompletion(node_id=0, peer_ids=[])
    actions = _drive_local_complete(rc, 0, received=10, expected=10, sent={0: 4})
    assert actions == [DownstreamEOF(C, expected_per_shard={0: 4})]


def test_eof_before_all_received_does_not_complete():
    rc = RingCompletion(node_id=0, peer_ids=[])
    rc.on_data(C)
    assert rc.on_upstream_eof(C, expected=5) == []  # only 1 of 5 received
    for _ in range(4):
        rc.on_data(C)
    # the controller re-checks after more data; emulate by re-sending eof state
    assert rc.on_upstream_eof(C, expected=5) == [Emit(C)]


def test_drop_forgets_a_client():
    rc = RingCompletion(node_id=0, peer_ids=[])
    rc.on_data(C)
    rc.on_data(C)
    rc.drop(C)
    # the client is forgotten: a later EOF sees a fresh, zeroed counter
    assert rc.on_upstream_eof(C, expected=2) == []
    rc.drop(C)  # idempotent


def test_three_nodes_barrier_sums_per_shard_then_leader_closes():
    leader = RingCompletion(node_id=0, peer_ids=[1, 2])
    # each peer sends to two downstream shards; the barrier sums them per shard
    [fwd] = _drive_local_complete(leader, 0, received=6, expected=6, sent={0: 2, 1: 1})
    assert isinstance(fwd, Forward)
    assert fwd.token.sent_by == {0: {0: 2, 1: 1}}

    peer1 = RingCompletion(node_id=1, peer_ids=[0, 2])
    _drive_local_complete(peer1, 1, received=6, expected=6, sent={0: 3, 1: 0})
    [fwd1] = peer1.on_token(fwd.token)
    assert isinstance(fwd1, Forward)

    peer2 = RingCompletion(node_id=2, peer_ids=[0, 1])
    _drive_local_complete(peer2, 2, received=6, expected=6, sent={0: 5, 1: 4})
    [fwd2] = peer2.on_token(fwd1.token)
    assert isinstance(fwd2, Forward)  # all present, but peer2 is not the leader

    [done] = leader.on_token(fwd2.token)
    assert done == DownstreamEOF(C, expected_per_shard={0: 2 + 3 + 5, 1: 1 + 0 + 4})


def test_redelivered_token_is_idempotent_no_double_close():
    leader = RingCompletion(node_id=0, peer_ids=[1])
    _drive_local_complete(leader, 0, received=4, expected=4, sent={0: 2})
    full = BarrierToken(C, origin=0, sent_by={0: {0: 2}, 1: {0: 7}})
    assert leader.on_token(full) == [DownstreamEOF(C, expected_per_shard={0: 9})]
    # the same token redelivered after a crash must NOT close again
    assert leader.on_token(full) == []


def test_snapshot_restore_preserves_phase_after_crash():
    rc = RingCompletion(node_id=0, peer_ids=[1])
    _drive_local_complete(rc, 0, received=4, expected=4, sent={0: 2})  # EMITTED
    snap = rc.snapshot_state()

    restored = RingCompletion(node_id=0, peer_ids=[1])
    restored.restore_state(snap)
    # a redelivered upstream EOF after restart must not re-emit (already EMITTED)
    assert restored.on_upstream_eof(C, expected=4) == []
    # the barrier still closes when the full token arrives, with the restored slot
    full = BarrierToken(C, origin=0, sent_by={0: {0: 2}, 1: {0: 1}})
    assert restored.on_token(full) == [DownstreamEOF(C, expected_per_shard={0: 3})]


def test_peer_that_sent_zero_still_participates():
    leader = RingCompletion(node_id=0, peer_ids=[1])
    _drive_local_complete(leader, 0, received=3, expected=3, sent={0: 0})
    [done] = leader.on_token(BarrierToken(C, origin=0, sent_by={0: {0: 0}, 1: {0: 4}}))
    assert done == DownstreamEOF(C, expected_per_shard={0: 4})


def test_non_leader_does_not_start_a_barrier():
    # only the leader (min id) circulates a token; a non-leader that completes just
    # transitions to EMITTED and waits to be collected -> exactly one DownstreamEOF.
    peer1 = RingCompletion(node_id=1, peer_ids=[0, 2])
    for _ in range(6):
        peer1.on_data(C)
    assert peer1.on_upstream_eof(C, expected=6) == [Emit(C)]
    assert peer1.report_sent(C, {0: 3}) == []
    assert peer1._client(C).phase == Phase.EMITTED


def test_token_does_not_count_a_peer_still_processing():
    # staleness guard: a token passing a peer that has not emitted yet must NOT
    # record its (stale, empty) slot, or the barrier would close with a wrong sum.
    peer1 = RingCompletion(node_id=1, peer_ids=[0, 2])
    peer1.on_data(C)  # still PROCESSING, no upstream EOF yet
    [fwd] = peer1.on_token(BarrierToken(C, origin=0, sent_by={0: {0: 5}}))
    assert isinstance(fwd, Forward)
    assert 1 not in fwd.token.sent_by


def test_leader_token_relaps_until_every_peer_has_emitted():
    # leader finishes first; its token must keep circulating (not close) until the
    # straggler peer emits, then close once with the correct total.
    leader = RingCompletion(node_id=0, peer_ids=[1])
    [fwd] = _drive_local_complete(leader, 0, received=4, expected=4, sent={0: 2})
    assert isinstance(fwd, Forward)  # only the leader's slot so far

    peer1 = RingCompletion(node_id=1, peer_ids=[0])
    # peer1 not done yet -> does not get counted, token comes back incomplete
    [still_fwd] = peer1.on_token(fwd.token)
    assert isinstance(still_fwd, Forward)
    assert 1 not in still_fwd.token.sent_by

    # peer1 emits, then the relapping token collects it and the leader closes once
    _drive_local_complete(peer1, 1, received=4, expected=4, sent={0: 7})
    [fwd1] = peer1.on_token(still_fwd.token)
    assert fwd1.token.sent_by == {0: {0: 2}, 1: {0: 7}}
    [done] = leader.on_token(fwd1.token)
    assert done == DownstreamEOF(C, expected_per_shard={0: 9})
