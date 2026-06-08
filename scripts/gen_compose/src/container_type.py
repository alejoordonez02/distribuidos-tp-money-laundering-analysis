from enum import StrEnum


class ContainerType(StrEnum):
    FILTER = "filter"
    GROUP_BY = "group_by"
    AGGREGATE = "aggregate"
    CONVERTER = "converter"
