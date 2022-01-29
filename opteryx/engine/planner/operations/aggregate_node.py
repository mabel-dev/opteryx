"""
Grouping Node

This is a SQL Query Execution Plan Node.

This performs aggregations.

This is a greedy operator.
"""
from calendar import c
import numpy as np
from typing import Iterable
from opteryx.engine import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode

# these functions can be applied to each group
INCREMENTAL_AGGREGATES = {
    "MIN": lambda x, y: min(np.min(x), y),
    "MAX": lambda x, y: max(np.max(x), y),
    "SUM": lambda x, y: np.sum(x) + y,
    "COUNT": lambda x, y: np.size(x) + y,
}

INCREMENTAL_AGGREGATES_SEEDS = {
    "MIN": np.min,
    "MAX": np.max,
    "SUM": np.sum,
    "COUNT": np.size,
}

# these functions need the whole dataset
WHOLE_AGGREGATES = {
    "avg": np.mean,
    "median": np.median,
    "distinct_count": lambda a: np.unique(a).size,
    "distinct_sets": lambda a: len(set(a)),
    "prod": np.prod,
    "std": np.std,
    "var": np.var,
    "first": lambda a: a[0],
    "last": lambda a: a[-1],
    # range - difference between min and max
    # percent - each group has the relative portion calculated
    # list - return a list of the items in the list
    # approx_distinct
    # quantile
}


def _incremental(x, y, function):
    if function in INCREMENTAL_AGGREGATES:
        return INCREMENTAL_AGGREGATES[function](x, y)
    return np.concatenate(x, y)


def _incremental_seed(x, function):
    if function in INCREMENTAL_AGGREGATES_SEEDS:
        return INCREMENTAL_AGGREGATES_SEEDS[function](x)
    return x


class AggregateNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._aggregates = config.get("aggregates")
        self._collector = {}

    def greedy(self):
        return True

    def execute(self, groups: Iterable) -> Iterable:

        for page in groups:
            for group in page:
                keys = {}
                if isinstance(group, tuple):
                    keys, group = group

                # We build the key value for the group collector
                group_identifier = tuple([(k, v) for k, v in keys.items()])

                for aggregrator in self._aggregates:

                    attribute = aggregrator["args"][0][0]
                    function = aggregrator["aggregate"]
                    column_name = f"{function}({attribute})"

                    # Add the responses to the collector
                    if group_identifier in self._collector:
                        self._collector[group_identifier][column_name] = _incremental(
                            group[attribute],
                            self._collector[group_identifier][column_name],
                            function,
                        )
                    else:
                        self._collector[group_identifier] = {}
                        self._collector[group_identifier][
                            column_name
                        ] = _incremental_seed(group[attribute], function)

                # TODO: if we're going to cap the number of groups we collect, do it here

        # TODO: do any whole aggregate functions

        print(self._collector)
