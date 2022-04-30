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
from typing import Iterable, List

import numpy as np
import pyarrow.json

from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.exceptions import SqlError
from opteryx.utils.columns import Columns

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
    "AGG_LIST": lambda a: list(a),  # all of the values in a column as a list
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
                if argument[1] == TOKEN_TYPES.IDENTIFIER or column == "*":
                    self._project.append(column)
                else:
                    raise SqlError("Can only aggregate on fields in the dataset.")
            elif "column_name" in attribute:
                self._project.append(attribute["column_name"])
            else:
                self._project.append(attribute["identifier"])

        self._project = [p for p in self._project if p is not None]

        # are we projecting by something not being grouped by?
        if not set(self._groups).issubset(self._project):
            raise SqlError(
                "All items in SELECT clause must be aggregates or included in the GROUP BY clause."
            )

        self._mapped_project: List = []
        self._mapped_groups: List = []

    @property
    def config(self):
        return str(self._aggregates)

    def greedy(self):
        return True

    @property
    def name(self):
        return "Aggregation"

    def execute(self, data_pages: Iterable) -> Iterable:

        from collections import defaultdict

        collector: dict = defaultdict(dict)
        columns = None

        if isinstance(data_pages, pyarrow.Table):
            data_pages = [data_pages]

        for page in data_pages:

            if columns is None:
                columns = Columns(page)

                for key in self._project:
                    if key != "*":
                        column = columns.get_column_from_alias(key, only_one=True)
                        if column not in self._mapped_project:
                            self._mapped_project.append(column)
                    else:
                        self._mapped_project.append("*")

                for group in self._groups:
                    self._mapped_groups.append(
                        columns.get_column_from_alias(group, only_one=True)
                    )

            for group in _map(page, self._mapped_project):

                for aggregrator in self._aggregates:

                    attribute = aggregrator["args"][0][0]
                    if attribute == "Wildcard":
                        mapped_attribute = "*"
                    else:
                        mapped_attribute = columns.get_column_from_alias(
                            attribute, only_one=True
                        )
                    function = aggregrator["aggregate"]
                    column_name = f"{function}({attribute})"
                    value = [v for k, v in group if k == mapped_attribute]
                    if len(value) == 1:
                        value = value[0]

                    # this is needed for situations where the select column isn't selecting
                    # the columns in the group by
                    # fmt:off
                    collection = tuple([(k,v,) for k, v in group if k in self._mapped_groups])
                    group_collector = collector[collection]
                    # fmt:on

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

        # count should return 0 rather than nothing
        if len(collector) == 0 and len(self._aggregates) == 1:
            if self._aggregates[0]["aggregate"] == "COUNT":
                collector = {(): {"COUNT(*)": 0}}

        buffer: List = []
        for collected, record in collector.items():
            if len(buffer) > 1000:
                table = pyarrow.Table.from_pylist(buffer)
                table = Columns.create_table_metadata(
                    table=table,
                    expected_rows=len(collector),
                    name=columns.table_name,
                    table_aliases=[],
                )
                yield table
                buffer = []
            for field, value in collected:
                mapped_field = columns.get_preferred_name(field)
                if hasattr(value, "as_py"):
                    value = value.as_py()  # type:ignore
                for agg in list(record.keys()):
                    if isinstance(agg, tuple):
                        func, col = agg
                        if func in WHOLE_AGGREGATES:
                            record[col] = WHOLE_AGGREGATES[func](
                                record.pop(agg)
                            )  # type:ignore
                record[mapped_field] = value
            buffer.append(record)

        if len(buffer) > 0:
            table = pyarrow.Table.from_pylist(buffer)
            table = Columns.create_table_metadata(
                table=table,
                expected_rows=len(collector),
                name="groupby",
                table_aliases=[],
            )
            yield table
