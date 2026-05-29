from common.comms.messages import TransactionCount, Transactions

from .group_by_fn import GroupByFn


class UC5CountGroupByFn(GroupByFn):
    def group_by(self, msg: Transactions) -> TransactionCount:  # type: ignore[reportIncompatibleMethodOverride]
        count = TransactionCount(msg.client_id, len(msg.transactions))
        return count
