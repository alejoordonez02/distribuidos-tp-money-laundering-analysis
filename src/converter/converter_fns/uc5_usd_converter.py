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

# Tasas de Bitcoin por día tomadas de investing.com, igual que en gen_input_output.
# La API de Frankfurter no provee Bitcoin; el fallback estático de
# FrankfurterConversionAPI no varía por día, por eso se sobreescribe aquí.
# Nota: el valor del 2022-09-02 (199999.0) proviene del notebook de la cátedra.
_BITCOIN_RATES_USD: dict[date, float] = {
    date(2022, 9, 1): 19793.1,
    date(2022, 9, 2): 199999.0,
    date(2022, 9, 3): 19831.4,
    date(2022, 9, 4): 19952.7,
    date(2022, 9, 5): 20126.1,
}


class UC5USDConverterFn(ConverterFn):
    def __init__(self, api: ConversionAPI):
        self._cache: dict[date, dict[str, float]] = {}
        for d in _PERIOD_A_DATES:
            rates = api.get_rates(d)
            if d in _BITCOIN_RATES_USD:
                rates["Bitcoin"] = _BITCOIN_RATES_USD[d]
            self._cache[d] = rates

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
