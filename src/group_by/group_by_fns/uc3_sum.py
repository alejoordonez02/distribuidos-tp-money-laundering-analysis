import logging
from uuid import UUID

from common.comms.messages import SumByPaymentFormat, Transactions

from .group_by_fn import GroupByFn


class UC3SumGroupByFn(GroupByFn):
    """Groups transactions by bank_id, keeping the max-amount entry per bank."""

    def __init__(self):
        self._state: dict[UUID, SumByPaymentFormat] = {}

    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = SumByPaymentFormat(msg.client_id, {})
        state = self._state[msg.client_id].sum_counts

        for t in msg.transactions:
            curr = state.get(t.payment_format)
            if curr is None:
                curr = (t.amount_paid, 1)
            else:
                curr = (curr[0] + t.amount_paid, curr[1] + 1)
            state[t.payment_format] = curr

    def get_result(self, client_id: UUID) -> SumByPaymentFormat:
        return self._state.get(client_id, SumByPaymentFormat(client_id, {}))
