from datetime import datetime

from common.comms.messages import Transactions

from .filter_fn import FilterFn

_FORMATS = {"Wire", "ACH"}
_PERIOD_A_START = datetime(2022, 9, 1)
_PERIOD_A_END = datetime(2022, 9, 6)  # exclusive upper bound


class UC5Filter(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = [
            t for t in el.transactions
            if t.payment_format in _FORMATS
            and _PERIOD_A_START <= t.timestamp < _PERIOD_A_END
        ]
        return Transactions(el.client_id, filtered)
