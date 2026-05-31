from collections import defaultdict
from typing import Iterable

from common.comms.messages import Accounts, BankNames

from .group_by_fn import GroupByFn

AFFINITY_SHARDS = 10


class UC2BankNamesGroupByFn(GroupByFn):
    """Groups accounts by bank_id, extracting the bank_name for each."""

    def group_by(self, msg: Accounts) -> Iterable[tuple[BankNames, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        bank_id_names = BankNames(
            msg.client_id,
            {a.bank_id: a.bank_name for a in msg.accounts},
        )

        affinities: dict[int, BankNames] = defaultdict(
            lambda: BankNames(msg.client_id, {})
        )

        for bank_id, bank_name in bank_id_names.data.items():
            affinity_shard = hash(bank_id) % AFFINITY_SHARDS
            affinities[affinity_shard].data[bank_id] = bank_name

        for affinity, bank_id_names in affinities.items():
            yield bank_id_names, affinity
