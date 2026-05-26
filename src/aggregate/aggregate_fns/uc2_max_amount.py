from uuid import UUID

from common.comms.messages import MaxByBank

from .aggregate_fn import AggregateFn


class UC2MaxAmountAggregateFn(AggregateFn):

    def __init__(self):
        self._state: dict[UUID, MaxByBank] = {}

    def aggregate(self, msg: MaxByBank):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = MaxByBank(msg.client_id, {})
        state = self._state[msg.client_id].data
        for bank_id, (account, amount) in msg.data.items():
            curr = state.get(bank_id)
            if curr is None or amount > curr[1]:
                state[bank_id] = (account, amount)

    def get_result(self, client_id: UUID) -> list[MaxByBank]:  # type: ignore[reportIncompatibleMethodOverride]
        return [self._state.pop(client_id, MaxByBank(client_id, {}))]
