from datetime import datetime

from common.comms.messages import Transactions

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"
_PERIOD_START = datetime(2022, 9, 1)
_PERIOD_END = datetime(2022, 9, 6)  # exclusive upper bound


class UC4Filter(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = [
            t
            for t in el.transactions
            if t.payment_currency == TARGET_CURRENCY
            and _PERIOD_START <= t.timestamp < _PERIOD_END
        ]
        return Transactions(el.client_id, filtered)
