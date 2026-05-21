from uuid import UUID

from common.comms.messages import TransactionCount, Transactions

from .group_by_fn import GroupByFn


class UC5TransactionCountGroupByFn(GroupByFn):
    def __init__(self):
        self._state: dict[UUID, TransactionCount] = {}

    def aggregate(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = TransactionCount(msg.client_id, 0)
        self._state[msg.client_id].count += len(msg.transactions)

    def get_result(self, client_id: UUID) -> TransactionCount:
        return self._state.get(client_id, TransactionCount(client_id, 0))
