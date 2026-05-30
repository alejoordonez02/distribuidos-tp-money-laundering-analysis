from typing import Iterable

from common.comms.messages import TransactionCount, Transactions

from .group_by_fn import GroupByFn


class UC5CountGroupByFn(GroupByFn):
    def group_by(self, msg: Transactions) -> Iterable[tuple[TransactionCount, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        count = TransactionCount(msg.client_id, len(msg.transactions))
        return ((count, hash(msg.client_id)),)
