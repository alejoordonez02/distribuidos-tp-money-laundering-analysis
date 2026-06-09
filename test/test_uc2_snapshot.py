import os
import sys
from uuid import uuid4

# The aggregate controller runs with src/aggregate on the path (no aggregate
# package), so mirror that here to import the fn.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "aggregate"))

from aggregate_fns.uc2_max_amount import UC2MaxAmountAggregateFn  # noqa: E402

from common.comms.messages import MaxByBank  # noqa: E402


def _feed(fn, client_id, data):
    fn.aggregate(MaxByBank(client_id, data))


def test_snapshot_restore_preserves_maxes():
    cid = uuid4()
    fn = UC2MaxAmountAggregateFn()
    _feed(fn, cid, {"bankA": ("acc1", 100.0)})
    _feed(fn, cid, {"bankA": ("acc2", 250.0), "bankB": ("acc3", 50.0)})

    restored = UC2MaxAmountAggregateFn()
    restored.restore_state(fn.snapshot_state())

    results = {
        bank: (acc, amt)
        for msg, _ in restored.get_result(cid)
        for bank, (acc, amt) in msg.data.items()
    }
    assert results == {"bankA": ("acc2", 250.0), "bankB": ("acc3", 50.0)}


def test_restore_then_aggregate_keeps_accumulating():
    cid = uuid4()
    fn = UC2MaxAmountAggregateFn()
    _feed(fn, cid, {"bankA": ("acc1", 100.0)})

    restored = UC2MaxAmountAggregateFn()
    restored.restore_state(fn.snapshot_state())
    _feed(restored, cid, {"bankA": ("acc2", 300.0)})

    results = dict(
        item for msg, _ in restored.get_result(cid) for item in msg.data.items()
    )
    assert results["bankA"] == ("acc2", 300.0)
