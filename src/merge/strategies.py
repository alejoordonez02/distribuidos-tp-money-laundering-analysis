from enum import StrEnum


class MergeStrategy(StrEnum):
    UC2_MERGE = "uc2_merge"
    UC3_MERGE = "uc3_merge"
    UC4_PRUNE = "uc4_prune"
