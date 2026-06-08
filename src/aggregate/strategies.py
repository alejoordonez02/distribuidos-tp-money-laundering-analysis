from enum import StrEnum


class AggregateStrategy(StrEnum):
    UC2_MAX_AMOUNT = "uc2_max_amount_aggregate"
    UC2_BANK_NAMES = "uc2_bank_names_aggregate"
    UC3_AVERAGE = "uc3_average_aggregate"
    UC4_COUNT_PATHS = "uc4_count_paths"
    UC4_AGGREGATE_GRAPHS = "uc4_aggregate_graphs"
    UC4_PATHS = "uc4_paths_aggregate"
    UC4_DEGREE = "uc4_degree_aggregate"
