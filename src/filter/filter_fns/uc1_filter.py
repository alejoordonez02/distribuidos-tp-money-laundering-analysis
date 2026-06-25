from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"
MIN_AMOUNT = 50.0


class UC1Filter(FilterFn):
    def _keep(self, t) -> bool:
        return t.payment_currency == TARGET_CURRENCY and t.amount_paid < MIN_AMOUNT
