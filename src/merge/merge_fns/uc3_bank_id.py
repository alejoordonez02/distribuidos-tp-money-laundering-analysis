from uuid import UUID

from common.comms.messages import MergedTransactions, AvgByFormat, Transactions
from common.data.transaction import Transaction

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

    def get_result(self, client_id: UUID) -> MergedTransactions:
        averages = self._averages.pop(client_id, {})
        transactions = self._transactions_to_merge.pop(client_id, [])
        entries = [t for t in transactions if t.payment_format in averages]
        return MergedTransactions(client_id, entries, averages)
