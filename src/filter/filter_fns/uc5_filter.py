from common.comms.messages import Transactions

from .filter_fn import FilterFn

_FORMATS = {"Wire", "ACH"}
_PERIOD_A_START = "2022-09-01 00:00:00"
_PERIOD_A_END = "2022-09-06 00:00:00"  # exclusive upper bound


class UC5Filter(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = [
            t for t in el.transactions
            if t.payment_format in _FORMATS
            and _PERIOD_A_START <= str(t.timestamp) < _PERIOD_A_END
        ]
        return Transactions(el.client_id, filtered)
