from datetime import datetime

from common.comms.messages import Transactions

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
BEGGINING_PERIOD = "2022-09-01 00:00:00"
END_PERIOD = "2022-09-05 23:59:59"


class UC3FilterPeriodA(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = []
        beggining_date = datetime.strptime(BEGGINING_PERIOD, DATETIME_FORMAT)
        end_date = datetime.strptime(END_PERIOD, DATETIME_FORMAT)
        for t in el.transactions:
            transaction_date = t.timestamp
            if (
                t.payment_currency == TARGET_CURRENCY
                and transaction_date >= beggining_date
                and transaction_date <= end_date
            ):
                filtered.append(t)
        return Transactions(el.client_id, filtered)
