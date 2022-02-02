"""
Grouping Node

This is a SQL Query Execution Plan Node.

This performs aggregations, both of grouped and non-grouped data. 

This is a greedy operator - it consumes all the data before responding.

This algorithm is a balance of performance, it is much slower than a groupby based on
the pyarrow_ops library for datasets with a high number of duplicate values (e.g.
grouping by a boolean column) - on a 10m record set, timings are 10:1 (removing raw
read time - e.g. 30s:21s where 20s is the read time).

But, on high cardinality data (nearly unique columns), the performance is much faster,
on a 10m record set, timings are 1:400 (50s:1220s where 20s is the read time).
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

JSON_TYPES = {
    np.bool_: bool,
    np.int64: int,
}

def _serializer(obj):
    return JSON_TYPES[type(obj)](obj)

def groupby(table, collect_columns):
    arr = [c.to_numpy() for c in table.select(list(collect_columns))]

    for row_index in range(len(arr[0])):
        ret = []
        for column_index, column_name in enumerate(collect_columns):
            ret.append((column_name, arr[column_index][row_index]))
        yield tuple(ret)


class AggregateNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._aggregates = []
        self._groups = config.get("groups", [])
        self._project = self._groups.copy()
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

        for page in groups:

            for group in groupby(page, self._groups):

                for aggregrator in self._aggregates:

                    attribute = aggregrator["args"][0][0]
                    function = aggregrator["aggregate"]
                    column_name = f"{function}({attribute})"

                    group_collector = collector[group]

                    # Add the responses to the collector
                    # if it's COUNT(*) 
                    if column_name == "COUNT(Wildcard)":
                        if "COUNT(*)" in group_collector:
                            group_collector["COUNT(*)"] += 1
                        else:
                            group_collector["COUNT(*)"] = 0
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

                    collector[group] = group_collector
                # TODO: if we're going to cap the number of groups we collect, do it here

        # TODO: do any whole aggregate functions

        import time

        buffer = bytearray()
        t = time.time_ns()
        for collected, record in collector.items():
            # we can't load huge json docs into pyarrow, so we chunk it
            if len(buffer) > (1024 * 1024):  # 1Mb - the default page size in mabel
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
