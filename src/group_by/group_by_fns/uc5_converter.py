import logging
import time
from dataclasses import replace
from datetime import date
from typing import Iterable

from common.comms.messages import Transactions
from common.conversion import ConversionAPI, ConversionAPIError

from .group_by_fn import GroupByFn

_USD_CURRENCY = "US Dollar"
_BASE_DELAY_SECS = 1.0
_MAX_DELAY_SECS = 30.0

# Bitcoin rates aren't in Frankfurter; injected from Binance BTCUSDT daily closes (same as gen_input_output)
_BITCOIN_RATES_USD: dict[date, float] = {
    date(2022, 9, 1): 20131.46,
    date(2022, 9, 2): 19951.86,
    date(2022, 9, 3): 19831.90,
    date(2022, 9, 4): 20000.30,
    date(2022, 9, 5): 19796.84,
}


class UC5ConverterGroupByFn(GroupByFn):
    """Converts each transaction's amount to USD, fetching a day's rates from the
    conversion API on a cache miss (one request per day, then cached). Forwards the
    converted batch to a single downstream shard chosen by message identity, so a
    re-emit after a crash lands on the same shard."""

    def __init__(
        self,
        api: ConversionAPI,
        base_delay: float = _BASE_DELAY_SECS,
        max_delay: float = _MAX_DELAY_SECS,
    ):
        self._api = api
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._cache: dict[date, dict[str, float]] = {}

    def group_by(self, msg: Transactions) -> Iterable[tuple[Transactions, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        converted = [
            replace(
                t,
                amount_paid=t.amount_paid * self._rates_for(t.timestamp.date()).get(t.payment_currency, 1.0),
                payment_currency=_USD_CURRENCY,
            )
            for t in msg.transactions
        ]
        affinity = hash((msg.producer_id, msg.seq))
        return ((Transactions(msg.client_id, converted), affinity),)

    def _rates_for(self, day: date) -> dict[str, float]:
        if day not in self._cache:
            rates = self._get_rates_with_retry(day)
            if day in _BITCOIN_RATES_USD:
                rates["Bitcoin"] = _BITCOIN_RATES_USD[day]
            self._cache[day] = rates
        return self._cache[day]

    def _get_rates_with_retry(self, day: date) -> dict[str, float]:
        delay = self._base_delay
        while True:
            try:
                return self._api.get_rates(day)
            except ConversionAPIError as e:
                logging.warning(
                    "conversion api unavailable (%s); retrying in %.1fs", e, delay
                )
                time.sleep(delay)
                delay = min(delay * 2, self._max_delay)
