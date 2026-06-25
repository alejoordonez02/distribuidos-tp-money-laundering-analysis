from .filter_fn import FilterFn

_USD_THRESHOLD = 1.0


class UC5AmountFilter(FilterFn):
    def _keep(self, t) -> bool:
        return t.amount_paid < _USD_THRESHOLD
