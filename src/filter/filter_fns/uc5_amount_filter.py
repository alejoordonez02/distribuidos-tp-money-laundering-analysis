from common.comms.messages import Transactions

from .filter_fn import FilterFn

_USD_THRESHOLD = 1.0


class UC5AmountFilter(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = [t for t in el.transactions if t.amount_paid < _USD_THRESHOLD]
        return Transactions(el.client_id, filtered)
