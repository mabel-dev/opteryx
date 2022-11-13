from enum import Enum

FUNCTION_KIND_FUNCTION: int = 1
FUNCTION_KIND_AGGREGATOR: int = 2


class FunctionProperty(int, Enum):
    NAME: int = 0
    TYPE: int = 1
    DESCRIPTION: int = 2
    RETURN_TYPE: int = 3
    NULLABLE_RETURN: int = 4  # can it ever return null
    MINIMUM_RETURN: int = 5  # lowest value that can be returned
    MAXIMUM_RETURN: int = 6  # greatest value that can be returned
    PARAMETERS: int = 7
    RELATED: int = 8  # functions that are related
    COST: int = 9  # indicative time to execute function 1 million times


# fmt:off
FUNCTION_REGISTRY = [
    ("ALL", 2, "Returns `true` if all items in group are true", bool, None, None, None, None, None, None),
    ("ANY", 2, "Returns `true` if any items in a group are true", bool, None, None, None, None, None, None),
]

"""
    "APPROXIMATE_MEDIAN": "approximate_median",
    "COUNT": "count",  # counts only non nulls
    "COUNT_DISTINCT": "count_distinct",
    "DISTINCT": "distinct",
    "LIST": "hash_list",
    "MAX": "max",
    "MAXIMUM": "max",  # alias
    "MEAN": "mean",
    "AVG": "mean",  # alias
    "AVERAGE": "mean",  # alias
    "MIN": "min",
    "MINIMUM": "min",  # alias
    "MIN_MAX": "min_max",
    "ONE": "hash_one",
    "ANY_VALUE": "hash_one",
    "PRODUCT": "product",
    "STDDEV": "stddev",
    "SUM": "sum",
    "VARIANCE": "variance",
"""

# fmt:on
