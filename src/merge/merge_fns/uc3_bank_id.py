import json
from datetime import datetime
from typing import Any, Iterator
from uuid import UUID

from common.checkpoint import PersistentSpill
from common.comms.messages import AvgByFormat, MergedTransactions, Transactions
from common.data.transaction import Transaction

from .merge_fn import MergeFn

_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Max transactions per emitted MergedTransactions. The right side is already
# spilled to disk, so get_result streams it back in bounded batches instead of
# loading every matching transaction into one list / one giant message (which
# OOM'd uc3_merge on the medium dataset: 3.76M txns ~= 4.7GB in RAM).
_CHUNK_SIZE = 10_000


def _serialize(t: Transaction) -> str:
    return json.dumps([
        t.timestamp.strftime(_DATETIME_FORMAT),
        t.from_bank, t.from_account,
        t.to_bank, t.to_account,
        t.amount_received, t.receiving_currency,
        t.amount_paid, t.payment_currency,
        t.payment_format,
    ])


def _deserialize(line: str) -> Transaction:
    f = json.loads(line)
    return Transaction(datetime.strptime(f[0], _DATETIME_FORMAT), *f[1:])


class UC3BankIdMergeFn(MergeFn):

    def __init__(self, spill: PersistentSpill):
        self._spill = spill
        self._averages: dict[UUID, dict[str, float]] = {}

    def left(self, msg: AvgByFormat):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._averages:
            self._averages[msg.client_id] = {}
        for fmt, avg in msg.averages.items():
            self._averages[msg.client_id][fmt] = avg

    def right(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        for t in msg.transactions:
            self._spill.append(msg.client_id, _serialize(t) + "\n")

    def get_result(self, client_id: UUID) -> Iterator[MergedTransactions]:  # type: ignore[reportIncompatibleMethodOverride]
        averages = self._averages.pop(client_id, {})

        # Stream the spilled right side back in bounded batches. Every chunk
        # carries the (small) averages dict so the downstream filter can process
        # each message independently — same data, same result as one big message.
        batch: list[Transaction] = []
        emitted = False
        for line in self._spill.iter_lines_and_clear(client_id):
            t = _deserialize(line.rstrip("\n"))
            if t.payment_format in averages:
                batch.append(t)
                if len(batch) >= _CHUNK_SIZE:
                    yield MergedTransactions(client_id, batch, averages)
                    emitted = True
                    batch = []

        if batch:
            yield MergedTransactions(client_id, batch, averages)
            emitted = True

        # Preserve the old contract of always emitting at least one message
        # (downstream expects >= 1 even when there are no matching transactions).
        if not emitted:
            yield MergedTransactions(client_id, [], averages)

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "averages": {str(c): v for c, v in self._averages.items()},
            "spill": self._spill.snapshot_state(),
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self._averages = {
            UUID(c): dict(v) for c, v in snapshot.get("averages", {}).items()
        }
        self._spill.restore_state(snapshot.get("spill", {}))
