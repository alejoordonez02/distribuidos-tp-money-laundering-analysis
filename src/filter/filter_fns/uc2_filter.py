from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"


class UC2Filter(FilterFn):
    def _keep(self, t) -> bool:
        return t.payment_currency == TARGET_CURRENCY
