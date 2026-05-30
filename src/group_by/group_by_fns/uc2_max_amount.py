from typing import Iterable

from common.comms.messages import MaxByBank, Transactions

from .group_by_fn import GroupByFn

AFFINITY_SHARDS = 100


class UC2MaxAmountGroupByFn(GroupByFn):
    """Groups transactions by bank_id, keeping the max-amount entry per bank."""

    def group_by(self, msg: Transactions) -> Iterable[tuple[MaxByBank, int | None]]:  # type: ignore[reportIncompatibleMethodOverride]
        maxes_by_bank = MaxByBank(msg.client_id, {})

        for t in msg.transactions:
            curr_max = maxes_by_bank.data.get(t.from_bank)

            if not curr_max or t.amount_paid > curr_max[1]:
                new_max = (t.from_account, t.amount_paid)
                maxes_by_bank.data[t.from_bank] = new_max

        # TODO: esto seguro se puede hacer mejor pero
        #       estoy sólo tocando affinities.
        #       Probablemente se pueda hacer en el
        #       mismo loop q arriba.
        affinities: dict[int, tuple[str, tuple[str, float]]] = {}

        for bank_id, (account, max2) in maxes_by_bank.data.items():
            affinity_shard = hash(bank_id) % AFFINITY_SHARDS
            affinities[affinity_shard] = bank_id, (account, max2)

        for affinity, (bank_id, (account, max2)) in affinities.items():
            bank_max = MaxByBank(msg.client_id, {bank_id: (account, max2)})
            yield bank_max, affinity
