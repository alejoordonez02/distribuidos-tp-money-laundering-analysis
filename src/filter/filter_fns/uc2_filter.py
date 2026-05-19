from common.comms.messages import Transactions

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"


class UC2Filter(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = [t for t in el.transactions if t.payment_currency == TARGET_CURRENCY]
        return Transactions(el.client_id, filtered)
