from enum import StrEnum


class GroupByStrategy(StrEnum):
    UC2_MAX_AMOUNT = "uc2_max_amount"
    UC2_BANK_NAMES = "uc2_bank_names"
    UC3_SUM = "uc3_sum"
    UC4_COMPUTE_GRAPH = "uc4_compute_graph"
    UC5_COUNT = "uc5_count"
