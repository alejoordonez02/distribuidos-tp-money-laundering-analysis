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


def test_data_after_eof_completes_when_expected_reached():
    # When a multi-peer upstream feeds one shard, the EOF (carrying `expected`) can arrive before the last data, so on_data must re-check completion: the message that reaches `expected` is data, not the EOF.
    rc = RingCompletion(node_id=0, peer_ids=[])
    rc.on_data(C)
    assert rc.on_upstream_eof(C, expected=3) == []  # only 1 of 3 received
    assert rc.on_data(C) == []  # 2 of 3
    assert rc.on_data(C) == [Emit(C)]  # 3rd data reaches expected -> emit
    # idempotent once emitted: late duplicate data must not re-emit
    rc.report_sent(C, {0: 1})
    assert rc.on_data(C) == []


def test_drop_forgets_a_client():
    rc = RingCompletion(node_id=0, peer_ids=[])
    rc.on_data(C)
    rc.on_data(C)
    rc.drop(C)
    # the client is forgotten: a later EOF sees a fresh, zeroed counter
    assert rc.on_upstream_eof(C, expected=2) == []
    rc.drop(C)  # idempotent


def test_drop_kills_a_circulating_token():
    # When a client aborts after its EOF started the barrier, a token is in flight; the drop must tombstone it so the relapping token dies instead of resurrecting a PROCESSING client and looping forever (live bug: token circulated default_filter's ring at 2 hops/s, never drained).
    leader = RingCompletion(node_id=0, peer_ids=[1])
    [fwd] = _drive_local_complete(leader, 0, received=4, expected=4, sent={0: 2})
    assert isinstance(fwd, Forward)  # a token is now circulating for C
    leader.drop(C)  # the abort arrives while the token is out
    assert leader.on_token(fwd.token) == []  # token dropped, not re-forwarded


def test_drop_ignores_late_data_and_eof():
    # tombstone is sticky: late data or a redelivered EOF for an aborted client must not resurrect it (client ids are gateway-minted per connection, never reused).
    rc = RingCompletion(node_id=0, peer_ids=[1])
    rc.drop(C)
    rc.on_data(C)
    assert rc.on_upstream_eof(C, expected=1) == []
    assert rc.on_token(BarrierToken(C, origin=0, sent_by={0: {0: 1}})) == []


def test_snapshot_restore_preserves_tombstone():
    # a node that crashes after an abort must still drop the client's token once restored, or the immortal-token loop reappears post-recovery.
    rc = RingCompletion(node_id=0, peer_ids=[1])
    rc.drop(C)
    restored = RingCompletion(node_id=0, peer_ids=[1])
    restored.restore_state(rc.snapshot_state())
    assert restored.on_token(BarrierToken(C, origin=0, sent_by={0: {0: 1}})) == []


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


def test_resolved_clients_lists_emitted_and_done():
    # A PROCESSING client is not resolved; once EMITTED its spilled state is safe to free. Guards the leak where a node revived after a crash past the emit never frees the spill (only the live EOF path did).
    rc = RingCompletion(node_id=0, peer_ids=[1])
    rc.on_data(C)
    assert rc.resolved_clients() == []  # still PROCESSING
    rc.on_upstream_eof(C, expected=1)  # received == expected
    rc.report_sent(C, {0: 1})  # -> EMITTED
    assert rc.resolved_clients() == [C]


def test_resolved_clients_includes_done_and_survives_restore():
    # A single node closes straight to DONE; the resolved set must survive a restore so a revived node frees the spill of a client that completed before it crashed.
    rc = RingCompletion(node_id=0, peer_ids=[])
    _drive_local_complete(rc, 0, received=4, expected=4, sent={0: 2})
    assert rc.resolved_clients() == [C]
    restored = RingCompletion(node_id=0, peer_ids=[])
    restored.restore_state(rc.snapshot_state())
    assert restored.resolved_clients() == [C]


def test_peer_that_sent_zero_still_participates():
    leader = RingCompletion(node_id=0, peer_ids=[1])
    _drive_local_complete(leader, 0, received=3, expected=3, sent={0: 0})
    [done] = leader.on_token(BarrierToken(C, origin=0, sent_by={0: {0: 0}, 1: {0: 4}}))
    assert done == DownstreamEOF(C, expected_per_shard={0: 4})


def test_non_leader_does_not_start_a_barrier():
    # only the leader (min id) circulates a token; a non-leader that completes transitions to EMITTED and waits to be collected -> exactly one DownstreamEOF.
    peer1 = RingCompletion(node_id=1, peer_ids=[0, 2])
    for _ in range(6):
        peer1.on_data(C)
    assert peer1.on_upstream_eof(C, expected=6) == [Emit(C)]
    assert peer1.report_sent(C, {0: 3}) == []
    assert peer1._client(C).phase == Phase.EMITTED


def test_token_does_not_count_a_peer_still_processing():
    # staleness guard: a token passing a peer that has not emitted yet must not record its (stale, empty) slot, or the barrier closes with a wrong sum.
    peer1 = RingCompletion(node_id=1, peer_ids=[0, 2])
    peer1.on_data(C)  # still PROCESSING, no upstream EOF yet
    [fwd] = peer1.on_token(BarrierToken(C, origin=0, sent_by={0: {0: 5}}))
    assert isinstance(fwd, Forward)
    assert 1 not in fwd.token.sent_by


def test_leader_token_relaps_until_every_peer_has_emitted():
    # leader finishes first; its token must keep circulating (not close) until the straggler peer emits, then close once with the correct total.
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
