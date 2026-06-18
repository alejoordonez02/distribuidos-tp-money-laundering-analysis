from dataclasses import replace
from datetime import date
from typing import Iterable

from common.comms.messages import Transactions
from common.conversion import ConversionAPI

from .group_by_fn import GroupByFn

_PERIOD_A_DATES = [
    date(2022, 9, 1),
    date(2022, 9, 2),
    date(2022, 9, 3),
    date(2022, 9, 4),
    date(2022, 9, 5),
]

_USD_CURRENCY = "US Dollar"

# Bitcoin rates not provided by Frankfurter API; injected from Binance BTCUSDT daily
# closing prices (investing.com source per professor email). Same values as gen_input_output.
_BITCOIN_RATES_USD: dict[date, float] = {
    date(2022, 9, 1): 20131.46,
    date(2022, 9, 2): 19951.86,
    date(2022, 9, 3): 19831.90,
    date(2022, 9, 4): 20000.30,
    date(2022, 9, 5): 19796.84,
}


class UC5ConverterGroupByFn(GroupByFn):
    """Converts each transaction's amount to USD using the day's rates, forwarding the
    converted batch to a single downstream shard chosen by message identity, so a
    re-emit after a crash lands on the same shard."""

    def __init__(self, api: ConversionAPI):
        self._cache: dict[date, dict[str, float]] = {}
        for d in _PERIOD_A_DATES:
            rates = api.get_rates(d)
            if d in _BITCOIN_RATES_USD:
                rates["Bitcoin"] = _BITCOIN_RATES_USD[d]
            self._cache[d] = rates

    def group_by(self, msg: Transactions) -> Iterable[tuple[Transactions, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        converted = [
            replace(
                t,
                amount_paid=t.amount_paid * self._cache.get(t.timestamp.date(), {}).get(t.payment_currency, 1.0),
                payment_currency=_USD_CURRENCY,
            )
            for t in msg.transactions
        ]
        affinity = hash((msg.producer_id, msg.seq))
        return ((Transactions(msg.client_id, converted), affinity),)
