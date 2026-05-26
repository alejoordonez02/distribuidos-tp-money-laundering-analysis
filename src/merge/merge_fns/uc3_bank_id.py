import logging
from uuid import UUID

from common.comms.messages import MergedTransactions, AvgByFormat, Transactions
from common.data.transaction import Transaction
from common.pipeline_config import UC3_MERGE_BATCH_SIZE

from .merge_fn import MergeFn


class UC3BankIdMergeFn(MergeFn):

    def __init__(self):
        self._transactions_to_merge: dict[UUID, list[Transaction]] = {}
        self._averages: dict[UUID, dict[str, float]] = {}

    def left(self, msg: AvgByFormat):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._averages:
            self._averages[msg.client_id] = {}

        avgs = self._averages[msg.client_id]
        for fmt, avg in msg.averages.items():
            avgs[fmt] = avg

    def right(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._transactions_to_merge:
            self._transactions_to_merge[msg.client_id] = []

        avgs = self._averages.get(msg.client_id)
        if avgs is not None:
            # averages already arrived: filter on the way in, don't buffer discards
            self._transactions_to_merge[msg.client_id].extend(
                t for t in msg.transactions if t.payment_format in avgs
            )
        else:
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
            if len(batch) >= UC3_MERGE_BATCH_SIZE:
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
