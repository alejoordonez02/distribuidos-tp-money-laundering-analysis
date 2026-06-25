from datetime import date

from .uc3_filter_period_base import UC3FilterPeriodBase

TARGET_PERIOD = (date(year=2022, month=9, day=6), date(year=2022, month=9, day=15))


class UC3FilterPeriodB(UC3FilterPeriodBase):
    TARGET_PERIOD = TARGET_PERIOD
