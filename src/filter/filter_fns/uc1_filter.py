from common.comms.messages.transaction import Transaction

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"
MIN_AMOUNT = 50.0


class UC1Filter(FilterFn[Transaction]):
    def filter(self, el: Transaction) -> bool:
        return (
            el.payment_currency != TARGET_CURRENCY or not el.amount_paid <= MIN_AMOUNT
        )
