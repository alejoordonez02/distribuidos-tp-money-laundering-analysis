from typing import Iterable

from common.comms.messages import SumByPaymentFormat, Transactions

from .group_by_fn import GroupByFn


class UC3SumGroupByFn(GroupByFn):
    def group_by(self, msg: Transactions) -> Iterable[tuple[SumByPaymentFormat, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        sum_counts: dict[str, tuple[float, int]] = {}

        for t in msg.transactions:
            if t.payment_format not in sum_counts:
                sum_counts[t.payment_format] = (0, 0)

            curr = sum_counts[t.payment_format]
            new = (curr[0] + t.amount_paid, curr[1] + 1)

            sum_counts[t.payment_format] = new

        for format2, (sum2, count) in sum_counts.items():
            sum_count = SumByPaymentFormat(msg.client_id, {format2: (sum2, count)})
            yield sum_count, hash(format2)
