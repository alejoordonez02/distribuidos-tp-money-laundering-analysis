import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Iterator
from uuid import UUID

from common.comms.messages import MergedTransactions, AvgByFormat, Transactions
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

    def __init__(self):
        self._files: dict[UUID, Path] = {}
        self._averages: dict[UUID, dict[str, float]] = {}

    def _file_for(self, client_id: UUID) -> Path:
        if client_id not in self._files:
            fd, path = tempfile.mkstemp(prefix=f"uc3_{client_id}_", suffix=".jsonl")
            os.close(fd)
            self._files[client_id] = Path(path)
        return self._files[client_id]

    def left(self, msg: AvgByFormat):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._averages:
            self._averages[msg.client_id] = {}
        for fmt, avg in msg.averages.items():
            self._averages[msg.client_id][fmt] = avg

    def right(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        path = self._file_for(msg.client_id)
        with open(path, "a") as f:
            for t in msg.transactions:
                f.write(_serialize(t) + "\n")

    def get_result(self, client_id: UUID) -> Iterator[MergedTransactions]:  # type: ignore[reportIncompatibleMethodOverride]
        averages = self._averages.pop(client_id, {})
        path = self._files.pop(client_id, None)

        # Stream the spilled right side back in bounded batches. Every chunk
        # carries the (small) averages dict so the downstream filter can process
        # each message independently — same data, same result as one big message.
        batch: list[Transaction] = []
        emitted = False
        if path and path.exists():
            with open(path) as f:
                for line in f:
                    t = _deserialize(line)
                    if t.payment_format in averages:
                        batch.append(t)
                        if len(batch) >= _CHUNK_SIZE:
                            yield MergedTransactions(client_id, batch, averages)
                            emitted = True
                            batch = []
            path.unlink()

        if batch:
            yield MergedTransactions(client_id, batch, averages)
            emitted = True

        # Preserve the old contract of always emitting at least one message
        # (downstream expects >= 1 even when there are no matching transactions).
        if not emitted:
            yield MergedTransactions(client_id, [], averages)
