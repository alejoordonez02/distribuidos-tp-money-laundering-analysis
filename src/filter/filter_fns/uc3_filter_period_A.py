from datetime import date

from .uc3_filter_period_base import UC3FilterPeriodBase

TARGET_PERIOD = (date(year=2022, month=9, day=1), date(year=2022, month=9, day=5))


class UC3FilterPeriodA(UC3FilterPeriodBase):
    TARGET_PERIOD = TARGET_PERIOD
