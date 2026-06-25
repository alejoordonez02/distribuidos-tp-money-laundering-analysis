import json
from datetime import datetime
from typing import Any, Iterator
from uuid import UUID

from common.checkpoint import PersistentSpill
from common.comms.messages import AvgByFormat, MergedTransactions, Transactions
from common.data.transaction import Transaction

from .merge_fn import MergeFn

_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# max txns per message: the right side is already on disk and streams back in bounded batches (loading it all OOM'd uc3_merge on medium: 3.76M txns ~= 4.7GB)
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

        # stream the spilled right side back in bounded batches; each chunk carries the small averages dict so the downstream filter processes it independently
        batch: list[Transaction] = []
        emitted = False
        for line in self._spill.iter_lines(client_id):
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

        # keep the contract of emitting at least one message (downstream expects >= 1 even with no matching transactions)
        if not emitted:
            yield MergedTransactions(client_id, [], averages)

    def discard(self, client_id: UUID):
        self._averages.pop(client_id, None)
        self._spill.clear(client_id)

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

    def clear_stale_spill(self):
        self._spill.clear_all()
