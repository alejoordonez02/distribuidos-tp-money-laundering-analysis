from typing import Iterable
from uuid import UUID

from common.comms.messages import MaxByBank

from .aggregate_fn import AggregateFn


class UC2MaxAmountAggregateFn(AggregateFn):
    def __init__(self):
        self.client_maxes_by_bank: dict[UUID, MaxByBank] = {}

    def aggregate(self, msg: MaxByBank):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.client_maxes_by_bank:
            self.client_maxes_by_bank[msg.client_id] = MaxByBank(msg.client_id, {})

        curr_maxes = self.client_maxes_by_bank[msg.client_id].data

        for bank_id, (account, amount) in msg.data.items():
            curr_max = curr_maxes.get(bank_id)

            if not curr_max or amount > curr_max[1]:
                new_max = (account, amount)
                curr_maxes[bank_id] = new_max

    def get_result(self, client_id: UUID) -> Iterable[tuple[MaxByBank, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        if client_id not in self.client_maxes_by_bank:
            return ()

        maxes_by_bank = self.client_maxes_by_bank.pop(client_id)

        for bank_id, (account, max2) in maxes_by_bank.data.items():
            # TODO: vamos a tener que hacer que todos los
            #       msjs sean unitarios
            bank_max = MaxByBank(client_id, {bank_id: (account, max2)})
            yield bank_max, hash(bank_id)
