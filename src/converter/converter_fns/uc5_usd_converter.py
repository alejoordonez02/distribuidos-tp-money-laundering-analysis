from dataclasses import replace

from common.comms.messages import Transactions
from common.conversion import ConversionAPI

from .converter_fn import ConverterFn

_PERIOD_A_DATES = [
    "2022-09-01",
    "2022-09-02",
    "2022-09-03",
    "2022-09-04",
    "2022-09-05",
]


class UC5USDConverterFn(ConverterFn):
    def __init__(self, api: ConversionAPI):
        self._cache = {date: api.get_rates(date) for date in _PERIOD_A_DATES}

    def convert(self, msg: Transactions) -> Transactions:
        converted = [
            replace(
                t,
                amount_paid=t.amount_paid * self._cache.get(str(t.timestamp)[:10], {}).get(t.payment_currency, 1.0),
                payment_currency="US Dollar",
            )
            for t in msg.transactions
        ]
        return Transactions(msg.client_id, converted)
