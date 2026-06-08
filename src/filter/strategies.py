from enum import StrEnum


class FilterStrategy(StrEnum):
    DEFAULT = "default"
    UC3_AVG = "uc3_avg"
    UC4_PATH = "uc4_path"
    UC5_AMOUNT = "uc5_amount"
