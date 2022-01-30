"""
Grouping Node

This is a SQL Query Execution Plan Node.

This performs aggregations.

This is a greedy operator - it consumes all the data before responding.
"""
import io
import orjson
import pyarrow.json
import numpy as np
from typing import Iterable
from opteryx.third_party.pyarrow_ops.group import Grouping
from opteryx.engine import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode

# these functions can be applied to each group
INCREMENTAL_AGGREGATES = {
    "MIN": lambda x, y: min(np.min(x), y),
    "MAX": lambda x, y: max(np.max(x), y),
    "SUM": lambda x, y: np.sum(x) + y,
    "COUNT": lambda x, y: np.size(x) + y,
}

# incremental updates usually need a suitable start
INCREMENTAL_AGGREGATES_SEEDS = {
    "MIN": np.min,
    "MAX": np.max,
    "SUM": np.sum,
    "COUNT": np.size,
}

# these functions need the whole dataset
WHOLE_AGGREGATES = {
    "AVG": np.mean,
    "MEDIAN": np.median,
    "PRODUCT": np.prod,
    "STDDEV_POP": np.std,
    "VAR_POP": np.var,
    "FIRST": lambda a: a[0],
    "LAST": lambda a: a[-1],
    # range - difference between min and max
    # percent - each group has the relative portion calculated
    # list - return a list of the items in the list
    # quantile
    # approx_distinct
    # approx_quantile
}


def _incremental(x, y, function):
    if function in INCREMENTAL_AGGREGATES:
        return INCREMENTAL_AGGREGATES[function](x, y)
    return np.concatenate(x, y)


def _incremental_seed(x, function):
    if function in INCREMENTAL_AGGREGATES_SEEDS:
        return INCREMENTAL_AGGREGATES_SEEDS[function](x)
    return x


def _serializer(obj):
    if isinstance(obj, np.int64):
        return int(obj)


class AggregateNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._aggregates = []
        self._project = config.get("groups", [])
        aggregates = config.get("aggregates", [])
        print(aggregates)
        for attribute in aggregates:
            if "aggregate" in attribute:
                self._aggregates.append(attribute)
            else:
                self._project.append(attribute)

        self._project = list(set(self._project))

    def __repr__(self):
        return str(self._aggregates)

    def greedy(self):
        return True

    def execute(self, groups: Iterable) -> Iterable:

        from collections import defaultdict

        collector = defaultdict(dict)

        # if we're not running this after a Group By, we need to add a layer
        if not isinstance(groups, Grouping):
            groups = [groups]

        for page in groups:
            for group in page:
                # We build the key value for the group collector
                group_identifier = tuple(
                    [(col, group[col][0]) for col in self._project]
                )

                for aggregrator in self._aggregates:

                    attribute = aggregrator["args"][0][0]
                    function = aggregrator["aggregate"]
                    column_name = f"{function}({attribute})"

                    group_collector = collector[group_identifier]

                    # Add the responses to the collector
                    # if it's COUNT(*) - we have a shortcut
                    if column_name == "COUNT(Wildcard)":
                        count = group_collector.get("COUNT(*)", 0)
                        group_collector["COUNT(*)"] = count + group.num_rows
                    # if we have some information collected for this group,
                    # incremental update
                    elif column_name in group_collector:
                        group_collector[column_name] = _incremental(
                            group[attribute],
                            group_collector[column_name],
                            function,
                        )
                    # otherwise, it's new, so seed the collection
                    else:
                        group_collector[column_name] = _incremental_seed(
                            group[attribute], function
                        )

                    collector[group_identifier] = group_collector
                # TODO: if we're going to cap the number of groups we collect, do it here

        # TODO: do any whole aggregate functions

        import time

        buffer = bytearray()
        t = time.time_ns()
        for collected, record in collector.items():
            # we can't load huge json docs into pyarrow, so we chunk it
            if len(buffer) > (1024 * 1024) :  # 1Mb - the default page size in mabel
                table = pyarrow.json.read_json(io.BytesIO(buffer))
                yield table
                buffer = bytearray()
            for k, v in collected:
                if hasattr(v, "as_py"):
                    v = v.as_py()
                record[k] = v
            buffer.extend(orjson.dumps(record, default=_serializer))

        if len(buffer) > 0:
            table = pyarrow.json.read_json(io.BytesIO(buffer))
            yield table

        print("building group table", (time.time_ns() - t) / 1e9)
