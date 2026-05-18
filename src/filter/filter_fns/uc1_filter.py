from common.comms.messages import Transactions

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"
MIN_AMOUNT = 50.0


class UC1Filter(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = []
        for t in el.transactions:
            if t.payment_currency == TARGET_CURRENCY or t.amount_paid <= MIN_AMOUNT:
                filtered.append(t)

        return Transactions(el.client_id, filtered)
