from uuid import UUID

from common.comms.messages import BankNames

from .aggregate_fn import AggregateFn


class UC2BankNamesAggregateFn(AggregateFn):

    def __init__(self):
        self._state: dict[UUID, BankNames] = {}

    def aggregate(self, msg: BankNames):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = BankNames(msg.client_id, {})
        self._state[msg.client_id].data.update(msg.data)

    def get_result(self, client_id: UUID) -> BankNames:  # type: ignore[reportIncompatibleMethodOverride]
        return self._state.pop(client_id, BankNames(client_id, {}))
