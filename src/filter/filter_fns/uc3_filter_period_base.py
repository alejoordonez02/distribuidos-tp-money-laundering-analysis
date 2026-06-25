from datetime import date

from .filter_fn import FilterFn

TARGET_CURRENCY = "US Dollar"


class UC3FilterPeriodBase(FilterFn):
    """
    Keep USD transactions whose date falls within ``TARGET_PERIOD`` (both ends
    inclusive). Concrete subclasses only fix their ``TARGET_PERIOD``.
    """

    TARGET_PERIOD: tuple[date, date]

    def _keep(self, t) -> bool:
        return (
            t.payment_currency == TARGET_CURRENCY
            and self.TARGET_PERIOD[0] <= t.timestamp.date() <= self.TARGET_PERIOD[1]
        )
