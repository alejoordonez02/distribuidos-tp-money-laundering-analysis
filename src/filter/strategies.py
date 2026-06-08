from enum import StrEnum


class FilterStrategy(StrEnum):
    DEFAULT = "default"
    UC3_AVG = "uc3_average_filter"
    UC4_PATH = "uc4_path"
    UC5_AMOUNT = "uc5_amount_filter"
