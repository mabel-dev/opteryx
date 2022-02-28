# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from opteryx.engine import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode

# these functions can be applied to each group
INCREMENTAL_AGGREGATES = {
    "MIN": lambda x, y: min(x, y),
    "MAX": lambda x, y: max(x, y),
    "SUM": lambda x, y: x + y,
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


JSON_TYPES = {np.bool_: bool, np.int64: int, np.float64: float}


def _serializer(obj):
    return JSON_TYPES[type(obj)](obj)


def _map(table, collect_columns):
    # if we're aggregating on * we don't care about the data, just the number of
    # records
    if collect_columns == ["*"]:
        for i in range(table.num_rows):
            yield (("*", "*"),)
        return

    if "*" in collect_columns:
        collect_columns = [c for c in collect_columns if c != "*"]
    arr = [c.to_numpy() for c in table.select(collect_columns)]

    for row_index in range(len(arr[0])):
        ret = []
        for column_index, column_name in enumerate(collect_columns):
            ret.append((column_name, arr[column_index][row_index]))
        yield ret


class AggregateNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):

        from opteryx.engine.attribute_types import TOKEN_TYPES

        self._aggregates = []
        self._groups = config.get("groups", [])
        self._project = self._groups.copy()
        aggregates = config.get("aggregates", [])
        for attribute in aggregates:
            if "aggregate" in attribute:
                self._aggregates.append(attribute)
                argument = attribute["args"][0]
                column = argument[0]
                if argument[1] == TOKEN_TYPES.WILDCARD:
                    column = "*"
                self._project.append(column)
            elif "column_name" in attribute:
                self._project.append(attribute["column_name"])
            else:
                self._project.append(attribute["identifier"])

        self._project = [p for p in self._project if p is not None]

    def __repr__(self):
        return str(self._aggregates)

    def greedy(self):
        return True

    def execute(self, groups: Iterable) -> Iterable:

        from collections import defaultdict

        collector: dict = defaultdict(dict)

        for page in groups:

            for group in _map(page, self._project):

                for aggregrator in self._aggregates:

                    attribute = aggregrator["args"][0][0]
                    function = aggregrator["aggregate"]
                    column_name = f"{function}({attribute})"
                    value = [v for k, v in group if k == attribute]
                    if len(value) == 1:
                        value = value[0]

                    # this is needed for situations where the select column isn't selecting
                    # the columns in the group by
                    collection = tuple(
                        [
                            (
                                k,
                                v,
                            )
                            for k, v in group
                            if k in self._groups
                        ]
                    )

                    group_collector = collector[collection]

                    # Add the responses to the collector if it's COUNT(*)
                    if column_name == "COUNT(Wildcard)":
                        if "COUNT(*)" in group_collector:
                            group_collector["COUNT(*)"] += 1
                        else:
                            group_collector["COUNT(*)"] = 1
                    elif function == "COUNT":
                        if value:
                            if column_name in group_collector:
                                group_collector[column_name] += 1
                            else:
                                group_collector[column_name] = 1
                    # if this is one of the functions we do an incremental aggregate
                    elif function in INCREMENTAL_AGGREGATES:
                        # if we have information about this collection
                        if column_name in group_collector:
                            group_collector[column_name] = _incremental(
                                value,
                                group_collector[column_name],
                                function,
                            )
                        # otherwise, it's new, so seed the collection with the initial value
                        else:
                            group_collector[column_name] = value
                    if function in WHOLE_AGGREGATES:
                        if (function, column_name) in group_collector:
                            group_collector[(function, column_name)].append(value)
                        else:
                            group_collector[(function, column_name)] = [value]

                    collector[collection] = group_collector

        import time

        from pyarrow.json import ReadOptions

        ro = ReadOptions(block_size=3 * 1024 * 1024)

        buffer = bytearray()
        t = time.time_ns()
        for collected, record in collector.items():
            # we can't load huge json docs into pyarrow, so we chunk it
            if len(buffer) > (2 * 1024 * 1024):  # 4Mb
                table = pyarrow.json.read_json(io.BytesIO(buffer), read_options=ro)
                yield table
                buffer = bytearray()
            for field, value in collected:
                if hasattr(value, "as_py"):
                    value = value.as_py()  # type:ignore
                for agg in list(record.keys()):
                    if isinstance(agg, tuple):
                        func, col = agg
                        if func in WHOLE_AGGREGATES:
                            record[col] = WHOLE_AGGREGATES[func](
                                record.pop(agg)
                            )  # type:ignore
                    else:
                        record[field] = value
            buffer.extend(orjson.dumps(record, default=_serializer))

        if len(buffer) > 0:
            table = pyarrow.json.read_json(io.BytesIO(buffer))
            yield table

        # timing over a yield is pointless
        # print("building group table", (time.time_ns() - t) / 1e9)
