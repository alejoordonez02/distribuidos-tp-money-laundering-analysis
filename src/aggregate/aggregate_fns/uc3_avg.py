import logging
from uuid import UUID

from common.comms.messages import AvgByFormat

from .aggregate_fn import AggregateFn


class UC3AvgAggregateFn(AggregateFn):
    def __init__(self):
        self._state: dict[UUID, AvgByFormat] = {}

    def aggregate(self, msg: SumByPaymentFormat):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = AvgByFormat(msg.client_id, {})
        state = self._state[msg.client_id].averages
        for format, (total_sum, transactions_amount) in msg.sum_amounts.items():
            curr = state.get(format, (0, 0))
            curr = (curr[0] + total_sum, curr[1] + transactions_amount)
            state[format] = curr

    def get_result(self, client_id: UUID) -> AvgByFormat:
        response = {}
        for format, (total_sum, transactions_amount) in self._state.get(
            client_id, AvgByFormat(client_id, {})
        ).averages.items():
            avg = total_sum / transactions_amount
            response[format] = avg

        msg = self._state.get(client_id)
        msg.averages = response
        return msg
