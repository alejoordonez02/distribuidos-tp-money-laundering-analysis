"""Unit tests for the per-UC business logic functions.

These functions were previously only exercised by the end-to-end pipeline. The
riskiest part of each one is the `snapshot_state`/`restore_state` round-trip
(home of past exactly-once bugs and the UC4 prune OOM), so every fn here gets a
`get_result`/`aggregate` correctness test PLUS a snapshot -> new instance ->
restore -> same result round-trip.

The fns are imported the same way the services run them: their source dir sits
on `sys.path` (added by `test/conftest.py`), so `aggregate_fns` / `merge_fns`
are top-level packages.
"""

from uuid import uuid4

from common.checkpoint.multi_shard_spill import MultiShardSpill
from common.checkpoint.persistent_spill import PersistentSpill
from common.comms.messages import (
    AvgByFormat,
    BankNames,
    Graph,
    HighDegree,
    MaxByBank,
    MergedBankData,
    Node,
    PathCounts,
    SumByPaymentFormat,
)
from common.comms.messages.graph_src import Path

from aggregate_fns.uc3_avg import UC3AvgAggregateFn  # noqa: E402
from aggregate_fns.uc4_count_paths import UC4CountPaths  # noqa: E402
from merge_fns.uc2_bank_id import UC2BankIdMergeFn  # noqa: E402
from merge_fns.uc4_prune import UC4PruneMergeFn  # noqa: E402

CID = uuid4()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _node(bank: str, account: str) -> Node:
    return Node(bank, account)


def _collect_path_counts(result) -> dict[str, int]:
    """Flatten the (PathCounts, affinity) stream into {str(path): summed count}."""
    out: dict[str, int] = {}
    for path_counts, _affinity in result:
        assert isinstance(path_counts, PathCounts)
        for path, count in path_counts.counts.items():
            out[str(path)] = out.get(str(path), 0) + count
    return out


def _collect_avgs(result) -> dict[str, float]:
    out: dict[str, float] = {}
    for avg, _hash in result:
        assert isinstance(avg, AvgByFormat)
        out.update(avg.averages)
    return out


def _collect_prune_nodes(result) -> dict[str, tuple[set[str], set[str]]]:
    """Flatten the Graph stream into {node.key: (pred keys, succ keys)}."""
    out: dict[str, tuple[set[str], set[str]]] = {}
    for graph in result:
        assert isinstance(graph, Graph)
        for node, (preds, succs) in graph.nodes.items():
            p, s = out.setdefault(node.key, (set(), set()))
            p.update(n.key for n in preds)
            s.update(n.key for n in succs)
    return out


# ===========================================================================
# UC2  --  max-by-bank merge + bank names, float reparse on restore
# ===========================================================================
def test_uc2_keeps_max_amount_per_bank_and_only_named_banks():
    fn = UC2BankIdMergeFn()

    # bank "1": 100 then 200 (higher -> replaces); bank "2": 50 then 30 (lower -> keeps)
    fn.left(MaxByBank(CID, {"1": ("accA", 100.0), "2": ("accB", 50.0)}))
    fn.left(MaxByBank(CID, {"1": ("accX", 200.0), "2": ("accY", 30.0)}))
    # bank "9" has a name but no amount -> excluded; bank "3" amount-less name present
    fn.right(BankNames(CID, {"1": "BankOne", "2": "BankTwo", "9": "BankNine"}))

    [merged] = list(fn.get_result(CID))
    assert isinstance(merged, MergedBankData)
    by_bank = {e[0]: e for e in merged.entries}

    assert by_bank["1"] == ("1", "accX", 200.0, "BankOne")  # max won
    assert by_bank["2"] == ("2", "accB", 50.0, "BankTwo")  # first (higher) kept
    assert "9" not in by_bank  # name without amount excluded


def test_uc2_excludes_bank_without_name():
    fn = UC2BankIdMergeFn()
    fn.left(MaxByBank(CID, {"1": ("a", 10.0), "7": ("b", 99.0)}))
    fn.right(BankNames(CID, {"1": "Named"}))

    [merged] = list(fn.get_result(CID))
    banks = {e[0] for e in merged.entries}
    assert banks == {"1"}  # bank "7" has an amount but no name -> dropped


def test_uc2_get_result_consumes_client():
    fn = UC2BankIdMergeFn()
    fn.left(MaxByBank(CID, {"1": ("a", 10.0)}))
    fn.right(BankNames(CID, {"1": "Named"}))

    first = list(fn.get_result(CID))
    assert first[0].entries  # got data
    # second call: state popped -> empty (single yielded MergedBankData with no entries)
    [again] = list(fn.get_result(CID))
    assert again.entries == []


def test_uc2_snapshot_restore_roundtrip_reparses_floats():
    fn = UC2BankIdMergeFn()
    fn.left(MaxByBank(CID, {"1": ("accX", 200.5), "2": ("accB", 50.0)}))
    fn.right(BankNames(CID, {"1": "BankOne", "2": "BankTwo"}))

    snap = fn.snapshot_state()

    restored = UC2BankIdMergeFn()
    restored.restore_state(snap)

    [merged] = list(restored.get_result(CID))
    by_bank = {e[0]: e for e in merged.entries}
    assert by_bank["1"] == ("1", "accX", 200.5, "BankOne")
    assert by_bank["2"] == ("2", "accB", 50.0, "BankTwo")
    # amounts came back as real floats, not strings
    assert all(isinstance(e[2], float) for e in merged.entries)


# ===========================================================================
# UC4 prune  --  intersect graph edges with high-degree sets (PersistentSpill)
# ===========================================================================
def _uc4_prune_setup(fn: UC4PruneMergeFn):
    A, B = _node("b", "A"), _node("b", "B")  # high out-degree predecessors
    C, D = _node("b", "C"), _node("b", "D")  # high in-degree successors
    X, Y = _node("b", "X"), _node("b", "Y")  # low-degree, must be pruned away

    fn.left(HighDegree(CID, hi_out={A, B}, hi_in={C, D}))
    # N: kept, preds/succs filtered down to the hi-degree members
    fn.right(Graph(CID, {_node("b", "N"): ({A, X}, {C, Y})}))
    # M: predecessors miss hi_out entirely -> dropped
    fn.right(Graph(CID, {_node("b", "M"): ({X}, {C})}))
    # K: successors miss hi_in entirely -> dropped
    fn.right(Graph(CID, {_node("b", "K"): ({A}, {Y})}))
    return A, C


def test_uc4_prune_filters_to_high_degree_intersection(tmp_path):
    spill = PersistentSpill(str(tmp_path), "prune")
    fn = UC4PruneMergeFn(spill)
    A, C = _uc4_prune_setup(fn)

    nodes = _collect_prune_nodes(fn.get_result(CID))

    assert set(nodes) == {_node("b", "N").key}  # only N survives
    preds, succs = nodes[_node("b", "N").key]
    assert preds == {A.key}  # X pruned (not in hi_out)
    assert succs == {C.key}  # Y pruned (not in hi_in)


def test_uc4_prune_snapshot_restore_roundtrip(tmp_path):
    spill1 = PersistentSpill(str(tmp_path), "prune")
    fn1 = UC4PruneMergeFn(spill1)
    _uc4_prune_setup(fn1)

    snap = fn1.snapshot_state()

    # fresh instance over the SAME directory (spill files persist on disk)
    spill2 = PersistentSpill(str(tmp_path), "prune")
    fn2 = UC4PruneMergeFn(spill2)
    fn2.restore_state(snap)

    nodes = _collect_prune_nodes(fn2.get_result(CID))
    assert set(nodes) == {_node("b", "N").key}
    preds, succs = nodes[_node("b", "N").key]
    assert preds == {_node("b", "A").key}
    assert succs == {_node("b", "C").key}


# ===========================================================================
# UC4 count_paths  --  count a->c pairs, spill to shards, sum partials
# ===========================================================================
def _uc4_count_setup(fn: UC4CountPaths):
    A, B = _node("b", "A"), _node("b", "B")
    C = _node("b", "C")
    # node N: preds {A,B} succs {C}  ->  paths A->C, B->C
    fn.aggregate(Graph(CID, {_node("b", "N"): ({A, B}, {C})}))
    # second graph repeats A->C  -> its count becomes 2
    fn.aggregate(Graph(CID, {_node("b", "M"): ({A}, {C})}))
    # self-loop A->A must be skipped (a != c guard)
    fn.aggregate(Graph(CID, {_node("b", "S"): ({A}, {A})}))


def test_uc4_count_paths_sums_and_skips_self_loops(tmp_path):
    spill = MultiShardSpill(str(tmp_path), "cp")
    fn = UC4CountPaths(spill)
    _uc4_count_setup(fn)

    counts = _collect_path_counts(fn.get_result(CID))

    ac = str(Path(_node("b", "A"), _node("b", "C")))
    bc = str(Path(_node("b", "B"), _node("b", "C")))
    aa = str(Path(_node("b", "A"), _node("b", "A")))
    assert counts[ac] == 2  # seen in N and M
    assert counts[bc] == 1
    assert aa not in counts  # self-loop dropped


def test_uc4_count_paths_snapshot_restore_roundtrip(tmp_path):
    spill1 = MultiShardSpill(str(tmp_path), "cp")
    fn1 = UC4CountPaths(spill1)
    _uc4_count_setup(fn1)

    snap = fn1.snapshot_state()  # flushes in-memory partials to disk shards

    spill2 = MultiShardSpill(str(tmp_path), "cp")
    fn2 = UC4CountPaths(spill2)
    fn2.restore_state(snap)

    counts = _collect_path_counts(fn2.get_result(CID))
    ac = str(Path(_node("b", "A"), _node("b", "C")))
    bc = str(Path(_node("b", "B"), _node("b", "C")))
    assert counts[ac] == 2
    assert counts[bc] == 1


# ===========================================================================
# UC3 avg  --  accumulate (sum, count) per payment format, divide on result
# ===========================================================================
def test_uc3_avg_accumulates_sum_and_count():
    fn = UC3AvgAggregateFn()
    fn.aggregate(SumByPaymentFormat(CID, {"ACH": (100.0, 2)}))
    fn.aggregate(SumByPaymentFormat(CID, {"ACH": (50.0, 3), "Wire": (20.0, 1)}))

    avgs = _collect_avgs(fn.get_result(CID))
    assert avgs["ACH"] == 150.0 / 5  # (100+50) / (2+3)
    assert avgs["Wire"] == 20.0


def test_uc3_avg_unknown_client_yields_nothing():
    fn = UC3AvgAggregateFn()
    assert list(fn.get_result(uuid4())) == []


def test_uc3_avg_get_result_consumes_client():
    fn = UC3AvgAggregateFn()
    fn.aggregate(SumByPaymentFormat(CID, {"ACH": (10.0, 1)}))
    assert _collect_avgs(fn.get_result(CID)) == {"ACH": 10.0}
    assert list(fn.get_result(CID)) == []  # popped on first read


def test_uc3_avg_snapshot_restore_roundtrip_reparses_types():
    fn = UC3AvgAggregateFn()
    fn.aggregate(SumByPaymentFormat(CID, {"ACH": (100.0, 2), "Wire": (20.0, 1)}))
    fn.aggregate(SumByPaymentFormat(CID, {"ACH": (50.0, 3)}))

    snap = fn.snapshot_state()

    restored = UC3AvgAggregateFn()
    restored.restore_state(snap)

    # internal accumulator reparsed: sum -> float, count -> int
    sc = restored.sum_counts[CID].sum_counts
    assert sc["ACH"] == (150.0, 5)
    assert isinstance(sc["ACH"][0], float) and isinstance(sc["ACH"][1], int)

    avgs = _collect_avgs(restored.get_result(CID))
    assert avgs["ACH"] == 150.0 / 5
    assert avgs["Wire"] == 20.0
