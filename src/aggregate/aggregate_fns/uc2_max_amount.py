from uuid import UUID

from common.comms.messages import MaxByBank

from .aggregate_fn import AggregateFn


class UC2MaxAmountAggregateFn(AggregateFn):

    def __init__(self):
        # client_id → {bank_id → (account, max_amount)}
        self._state: dict[UUID, dict[str, tuple[str, float]]] = {}

    def accumulate(self, msg: MaxByBank):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = {}
        state = self._state[msg.client_id]
        for bank_id, (account, amount) in msg.data.items():
            curr = state.get(bank_id)
            if curr is None or amount > curr[1]:
                state[bank_id] = (account, amount)

    def get_result(self, client_id: UUID) -> MaxByBank | None:
        return MaxByBank(client_id, self._state.get(client_id, {}))
