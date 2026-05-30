from common.comms.messages import SumByPaymentFormat, Transactions

from .group_by_fn import GroupByFn


class UC3SumGroupByFn(GroupByFn):
    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        sum_counts: dict[str, tuple[float, int]] = {}

        for t in msg.transactions:
            if t.payment_format not in sum_counts:
                sum_counts[t.payment_format] = (0, 0)

            curr = sum_counts[t.payment_format]
            new = (curr[0] + t.amount_paid, curr[1] + 1)

            sum_counts[t.payment_format] = new

        return SumByPaymentFormat(msg.client_id, sum_counts)
