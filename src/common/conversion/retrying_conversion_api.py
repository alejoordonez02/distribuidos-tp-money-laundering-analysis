import logging
import time
from datetime import date

from .conversion_api import ConversionAPI, ConversionAPIError

_BASE_DELAY_SECS = 1.0
_MAX_DELAY_SECS = 30.0


class RetryingConversionAPI(ConversionAPI):
    """Wraps a ConversionAPI, retrying on ConversionAPIError with capped exponential
    backoff. A transient outage of the upstream API stalls the node until it recovers
    instead of crashing it."""

    def __init__(
        self,
        inner: ConversionAPI,
        base_delay: float = _BASE_DELAY_SECS,
        max_delay: float = _MAX_DELAY_SECS,
    ):
        self._inner = inner
        self._base_delay = base_delay
        self._max_delay = max_delay

    def get_rates(self, day: date) -> dict[str, float]:
        delay = self._base_delay
        while True:
            try:
                return self._inner.get_rates(day)
            except ConversionAPIError as e:
                logging.warning(
                    "conversion api unavailable (%s); retrying in %.1fs", e, delay
                )
                time.sleep(delay)
                delay = min(delay * 2, self._max_delay)
