from enum import StrEnum


class GroupByStrategy(StrEnum):
    UC2_MAX_AMOUNT = "uc2_max_amount_group_by"
    UC2_BANK_NAMES = "uc2_bank_names_group_by"
    UC3_SUM = "uc3_group_by_format"
    UC4_COMPUTE_GRAPH = "uc4_compute_graph_group_by"
    UC4_DEGREE_COMPUTE_GRAPH = "uc4_degree_compute_graph"
    UC5_CONVERTER = "uc5_converter"
    UC5_COUNT = "uc5_count_group_by"
