from datetime import date

from common.comms.messages import Transactions

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"
TARGET_PERIOD = (date(year=2022, month=9, day=6), date(year=2022, month=9, day=15))


# TODO: este código está repetido en period a
class UC3FilterPeriodB(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = [
            t
            for t in el.transactions
            if t.payment_currency == TARGET_CURRENCY
            and TARGET_PERIOD[0] <= t.timestamp.date() <= TARGET_PERIOD[1]
        ]

        return Transactions(el.client_id, filtered)
