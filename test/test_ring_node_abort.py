from uuid import uuid4

from common.checkpoint.checkpoint_store import CheckpointStore
from common.checkpoint.checkpointer import Checkpointer
from common.checkpoint.deduplicator import Deduplicator
from common.comms.eof_handler.ring_completion import RingCompletion
from common.comms.eof_handler.ring_node import RingNode, StatelessRingNode
from common.comms.eof_handler.sent_counts import SentCounts
from common.comms.messages import Abort, deserialize_message


class FakeTx:
    def __init__(self):
        self.sent: list[bytes] = []

    def send(self, body: bytes):
        self.sent.append(body)


class FakeFn:
    def snapshot_state(self):
        return {}

    def restore_state(self, snapshot):
        pass


def _checkpointer(tmp_path):
    return Checkpointer(FakeFn(), CheckpointStore(str(tmp_path / "n.ckpt")), Deduplicator(), 10)


def _base_kwargs(rc, txs, checkpointer):
    return dict(
        node_id=0, rc=rc, consumer=None, ring=None, external_txs=txs,
        ring_queue="r", ring_exchange="r", data_prefetch=1, checkpointer=checkpointer,
        data_queue="d", data_exchange="d",
    )


def test_abort_drops_completion_tombstones_and_forwards(tmp_path):
    cid = uuid4()
    rc = RingCompletion(node_id=0, peer_ids=[])
    rc.on_data(cid)
    txs = [FakeTx(), FakeTx()]
    cp = _checkpointer(tmp_path)
    node = RingNode(**_base_kwargs(rc, txs, cp))

    node._on_abort(Abort(cid))

    assert cid not in rc._clients          # completion state dropped
    assert cp.is_aborted(cid)              # tombstoned so later data is ignored
    for tx in txs:                         # abort forwarded to every downstream
        assert len(tx.sent) == 1
        forwarded = deserialize_message(tx.sent[0])
        assert isinstance(forwarded, Abort)
        assert forwarded.client_id == cid


def test_stateless_abort_drops_sent_counts(tmp_path):
    cid = uuid4()
    rc = RingCompletion(node_id=0, peer_ids=[])
    sent = SentCounts()
    sent.add(cid, 0)
    node = StatelessRingNode(sent, **_base_kwargs(rc, [FakeTx()], _checkpointer(tmp_path)))

    node._on_abort(Abort(cid))

    assert sent.pop(cid) == {}             # already dropped by the abort
