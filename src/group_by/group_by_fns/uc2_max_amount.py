from uuid import UUID

from common.comms.messages import MaxByBank, Transactions

from .group_by_fn import GroupByFn


class UC2MaxAmountGroupByFn(GroupByFn):
    """Groups transactions by bank_id, keeping the max-amount entry per bank."""

    def __init__(self):
        self._state: dict[UUID, MaxByBank] = {}

    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = MaxByBank(msg.client_id, {})
        state = self._state[msg.client_id].data
        for t in msg.transactions:
            curr = state.get(t.from_bank)
            if curr is None or t.amount_paid > curr[1]:
                state[t.from_bank] = (t.from_account, t.amount_paid)

    def get_result(self, client_id: UUID) -> list[MaxByBank]:  # type: ignore[reportIncompatibleMethodOverride]
        return [self._state.get(client_id, MaxByBank(client_id, {}))]
