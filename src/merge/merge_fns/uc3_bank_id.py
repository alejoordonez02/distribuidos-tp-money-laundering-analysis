import logging
from uuid import UUID

from common.comms.messages import MergedTransactions, AvgByFormat, Transactions
from common.data.transaction import Transaction
from .merge_fn import MergeFn

_BATCH_SIZE = 10_000


class UC3BankIdMergeFn(MergeFn):

    def __init__(self):
        self._transactions_to_merge: dict[UUID, list[Transaction]] = {}
        self._averages: dict[UUID, dict[str, float]] = {}

    def left(self, msg: AvgByFormat):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._averages:
            self._averages[msg.client_id] = {}

        avgs = self._averages[msg.client_id]
        for (fmt, avg) in msg.averages.items():
            avgs[fmt] = avg

    def right(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._transactions_to_merge:
            self._transactions_to_merge[msg.client_id] = []

        self._transactions_to_merge[msg.client_id].extend(msg.transactions)

    def get_result(self, client_id: UUID) -> list[MergedTransactions]:
        averages = self._averages.pop(client_id, {})
        transactions = self._transactions_to_merge.pop(client_id, [])

        batches: list[MergedTransactions] = []
        batch: list[Transaction] = []

        for t in transactions:
            if t.payment_format not in averages:
                continue
            batch.append(t)
            if len(batch) >= _BATCH_SIZE:
                batches.append(MergedTransactions(client_id, batch, averages))
                batch = []

        if batch:
            batches.append(MergedTransactions(client_id, batch, averages))

        if not batches:
            batches.append(MergedTransactions(client_id, [], averages))

        logging.info(
            f"uc3 merge: emitting {len(batches)} batch(es) "
            f"({sum(len(b.transactions) for b in batches)} transactions)"
        )
        return batches
