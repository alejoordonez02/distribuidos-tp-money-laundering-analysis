import logging
from uuid import UUID

from common.comms.messages import MergedTransactions, AvgByFormat, Transactions
from common.data.transaction import Transaction
from .merge_fn import MergeFn


class UC3BankIdMergeFn(MergeFn):

    def __init__(self):
        self._transacions_to_merge: dict[UUID, list[Transaction]] = {}
        self._averages: dict[UUID, dict[str, float]] = {} # format: avg
        

    def left(self, msg: AvgByFormat):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._averages:
            self._averages[msg.client_id] = {}
        
        avgs = self._averages[msg.client_id]
        for (format, avg) in msg.data.items():
            avgs[format] = avg
        

    def right(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._transacions_to_merge:
            self._transacions_to_merge[msg.client_id] = []
        
        transactions = self._transacions_to_merge[msg.client_id]
        for t in msg.transactions:
            transactions.append(t)
        

    def get_result(self, client_id: UUID) -> MergedTransactions:
        averages = self._averages.get(client_id, {})
        transactions = self._transacions_to_merge.get(client_id, [])
        entries = []
        for t in transactions:
            entry = (t.from_bank, t.from_account, t.payment_format, t.amount_paid, averages[t.payment_format])
            entries.append(entry)
        return MergedTransactions(client_id, entries)
