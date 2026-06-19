from datetime import date

import pytest

from common.conversion import ConversionAPI, ConversionAPIError
from group_by.group_by_fns.uc5_converter import UC5ConverterGroupByFn

DAY = date(2022, 9, 1)
RATES = {"US Dollar": 1.0}


class _FlakyAPI(ConversionAPI):
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
    fn = UC5ConverterGroupByFn(inner, base_delay=0, max_delay=0)
    assert fn._get_rates_with_retry(DAY) == RATES
    assert inner.calls == 1


def test_retries_until_inner_succeeds():
    inner = _FlakyAPI(failures=3)
    fn = UC5ConverterGroupByFn(inner, base_delay=0, max_delay=0)
    assert fn._get_rates_with_retry(DAY) == RATES
    assert inner.calls == 4


def test_backoff_is_capped(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr("group_by.group_by_fns.uc5_converter.time.sleep", slept.append)
    inner = _FlakyAPI(failures=5)
    fn = UC5ConverterGroupByFn(inner, base_delay=1.0, max_delay=4.0)
    fn._get_rates_with_retry(DAY)
    assert slept == [1.0, 2.0, 4.0, 4.0, 4.0]


def test_non_conversion_errors_propagate():
    class _Broken(ConversionAPI):
        def get_rates(self, day: date) -> dict[str, float]:
            raise ValueError("boom")

    fn = UC5ConverterGroupByFn(_Broken(), base_delay=0, max_delay=0)
    with pytest.raises(ValueError):
        fn._get_rates_with_retry(DAY)
