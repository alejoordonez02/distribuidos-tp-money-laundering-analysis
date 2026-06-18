from datetime import date

import pytest

from common.conversion import (
    ConversionAPI,
    ConversionAPIError,
    RetryingConversionAPI,
)

DAY = date(2022, 9, 1)
RATES = {"US Dollar": 1.0}


class _FlakyAPI(ConversionAPI):
    """Fails the first `failures` calls with ConversionAPIError, then returns RATES."""

    def __init__(self, failures: int):
        self._remaining = failures
        self.calls = 0

    def get_rates(self, day: date) -> dict[str, float]:
        self.calls += 1
        if self._remaining > 0:
            self._remaining -= 1
            raise ConversionAPIError("api down")
        return RATES


def test_returns_inner_result_without_retry():
    inner = _FlakyAPI(failures=0)
    api = RetryingConversionAPI(inner, base_delay=0, max_delay=0)
    assert api.get_rates(DAY) == RATES
    assert inner.calls == 1


def test_retries_until_inner_succeeds():
    inner = _FlakyAPI(failures=3)
    api = RetryingConversionAPI(inner, base_delay=0, max_delay=0)
    assert api.get_rates(DAY) == RATES
    assert inner.calls == 4


def test_backoff_is_capped(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(
        "common.conversion.retrying_conversion_api.time.sleep", slept.append
    )
    inner = _FlakyAPI(failures=5)
    api = RetryingConversionAPI(inner, base_delay=1.0, max_delay=4.0)
    api.get_rates(DAY)
    assert slept == [1.0, 2.0, 4.0, 4.0, 4.0]


def test_non_conversion_errors_propagate():
    class _Broken(ConversionAPI):
        def get_rates(self, day: date) -> dict[str, float]:
            raise ValueError("boom")

    api = RetryingConversionAPI(_Broken(), base_delay=0, max_delay=0)
    with pytest.raises(ValueError):
        api.get_rates(DAY)
