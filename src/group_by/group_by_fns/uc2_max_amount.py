from typing import Iterable

from common.comms.messages import MaxByBank, Transactions

from .group_by_fn import GroupByFn


class UC2MaxAmountGroupByFn(GroupByFn):
    """Groups transactions by bank_id, keeping the max-amount entry per bank."""

    def group_by(self, msg: Transactions) -> Iterable[tuple[MaxByBank, int | None]]:  # type: ignore[reportIncompatibleMethodOverride]
        max_by_bank = MaxByBank(msg.client_id, {})

        for t in msg.transactions:
            curr_max = max_by_bank.data.get(t.from_bank)

            if not curr_max or t.amount_paid > curr_max[1]:
                new_max = (t.from_account, t.amount_paid)
                max_by_bank.data[t.from_bank] = new_max

        for bank_id, (account, max2) in max_by_bank.data.items():
            # TODO: vamos a tener que hacer que todos los
            #       msjs sean unitarios
            bank_max = MaxByBank(msg.client_id, {bank_id: (account, max2)})
            yield bank_max, hash(bank_id)
