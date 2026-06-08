from enum import StrEnum


class StatefulStrategy(StrEnum):
    UC2_MAX_AMOUNT_AGGREGATE = "uc2_max_amount_aggregate"
    UC2_BANK_NAMES_AGGREGATE = "uc2_bank_names_aggregate"
    UC3_AVERAGE_AGGREGATE = "uc3_average_aggregate"
    UC4_COUNT_PATHS = "uc4_count_paths"
    UC4_GRAPHS_AGGREGATE = "uc4_aggregate_graphs"
    UC4_PATHS_AGGREGATE = "uc4_paths_aggregate"
    UC4_DEGREE_AGGREGATE = "uc4_degree_aggregate"
