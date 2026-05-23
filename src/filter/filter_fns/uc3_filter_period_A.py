from datetime import datetime

from common.comms.messages import Transactions

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"
TARGET_PERIOD = (
    datetime(year=2022, month=9, day=1),
    datetime(year=2022, month=9, day=6),
)


# TODO: este código está repetido en period b
class UC3FilterPeriodA(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = [
            t
            for t in el.transactions
            if t.payment_currency == TARGET_CURRENCY
            and TARGET_PERIOD[0] <= t.timestamp <= TARGET_PERIOD[1]
        ]

        return Transactions(el.client_id, filtered)
