from uuid import UUID

from common.comms.messages import Count, Transactions

from .group_by_fn import GroupByFn


class UC5CountGroupByFn(GroupByFn):
    def __init__(self):
        self._state: dict[UUID, Count] = {}

    def aggregate(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = Count(msg.client_id, 0)
        self._state[msg.client_id].count += len(msg.transactions)

    def get_result(self, client_id: UUID) -> Count:
        return self._state.get(client_id, Count(client_id, 0))
