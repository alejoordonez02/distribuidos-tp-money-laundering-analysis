from datetime import datetime

from common.comms.messages import MergedTransactions, Transactions

from .filter_fn import FilterFn

PERCENTAGE = 0.01


class UC3AvgFilter(FilterFn):
    def filter(self, el: MergedTransactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = []
        for t in el.transactions:
            if (
                t.payment_format in el.averages
                and t.amount_paid < PERCENTAGE * el.averages[t.payment_format]
            ):
                filtered.append(t)

        return Transactions(el.client_id, filtered)
