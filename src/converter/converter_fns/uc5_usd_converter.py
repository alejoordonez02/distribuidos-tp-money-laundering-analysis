from dataclasses import replace
from datetime import date

from common.comms.messages import Transactions
from common.conversion import ConversionAPI

from .converter_fn import ConverterFn

_PERIOD_A_DATES = [
    date(2022, 9, 1),
    date(2022, 9, 2),
    date(2022, 9, 3),
    date(2022, 9, 4),
    date(2022, 9, 5),
]

_USD_CURRENCY = "US Dollar"


class UC5USDConverterFn(ConverterFn):
    def __init__(self, api: ConversionAPI):
        self._cache = {d: api.get_rates(d) for d in _PERIOD_A_DATES}

    def convert(self, msg: Transactions) -> Transactions:
        converted = [
            replace(
                t,
                amount_paid=t.amount_paid * self._cache.get(t.timestamp.date(), {}).get(t.payment_currency, 1.0),
                payment_currency=_USD_CURRENCY,
            )
            for t in msg.transactions
        ]
        return Transactions(msg.client_id, converted)
