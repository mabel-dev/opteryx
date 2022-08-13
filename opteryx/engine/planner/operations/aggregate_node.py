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
"""
import time

from typing import Iterable

import pyarrow

from opteryx.engine import QueryDirectives, QueryStatistics
from opteryx.engine.planner.expression import NodeType, evaluate_and_append, get_all_identifiers
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.exceptions import SqlError
from opteryx.utils.columns import Columns

COUNT_STAR: str = "COUNT(*)"

# use the aggregators from pyarrow
AGGREGATORS = {
    "ALL": "all",
    "ANY": "any",
    "APPROXIMATE_MEDIAN": "approximate_median",
    "COUNT": "count",  # counts only non nulls
    "COUNT_DISTINCT": "count_distinct",
    "CUMULATIVE_SUM": "cumulative_sum",
    "DISTINCT": "distinct",
    "LIST": "list",
    "MAX": "max",
    "MAXIMUM": "max",  # alias
    "MEAN": "mean",
    "AVG": "mean",  # alias
    "AVERAGE": "mean",  # alias
    "MIN": "min",
    "MINIMUM": "min",  # alias
    "MIN_MAX": "min_max",
    "ONE": "one",
    "PRODUCT": "product",
    "STDDEV": "stddev",
    "SUM": "sum",
    "QUANTILES": "tdigest",
    "VARIANCE": "variance",
}

def _is_count_star(aggregates, groups):
    """
    Is the SELECT clause `SELECT COUNT(*)` with no GROUP BY
    """
    if len(groups) != 0:
        return False
    if len(aggregates) != 1:
        return False
    if aggregates[0].value != "COUNT":
        return False
    if aggregates[0].parameters[0].token_type != NodeType.WILDCARD:
        return False
    return True

def _count_star(data_pages):
    count = 0
    for page in data_pages.execute():
        count += page.num_rows
    table = pyarrow.Table.from_pylist([{COUNT_STAR: count}])
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=1,
        name="groupby",
        table_aliases=[],
    )
    yield table

def _project(tables, fields):
    fields = set(fields)
    for table in tables:
        columns = Columns(table)
        column_names = [
            columns.get_column_from_alias(field, only_one=True) for field in fields
        ]
        yield table.select(set(column_names))

def _build_aggs(aggregators, columns):
    column_map = {}
    aggs = []

    for aggregator in aggregators:
        if aggregator.token_type == NodeType.AGGREGATOR:
            field_node = aggregator.parameters[0]
            if field_node.token_type == NodeType.WILDCARD:
                display_field = "*"
                field_name = columns.preferred_column_names[0][0]
            elif field_node.token_type == NodeType.IDENTIFIER:
                display_field = field_node.value
                field_name = columns.get_column_from_alias(field_node.value, only_one=True)
            else:
                raise SqlError("Invalid identifier provided in aggregator function `{field_name.value}`")
            function = AGGREGATORS.get(aggregator.value)
            aggs.append((field_name,function,))
            column_map[f"{aggregator.value.upper()}({display_field})"] = f"{field_name}_{function}"

    return column_map, aggs

def _non_group_aggregates(aggregates, table, columns):
    """
    If we're not doing a group by, we're just doing aggregations, the pyarrow
    functionality for aggregate doesn't work
    """

    result = {}

    for aggregate in aggregates:

        column_name = aggregate.parameters[0].value
        mapped_column_name = columns.get_column_from_alias(column_name, only_one=True)
        raw_column_values = table[mapped_column_name].to_numpy()
        aggregate_function_name = AGGREGATORS[aggregate.value]
        # this maps a string which is the function name to the function
        aggregate_function = getattr(pyarrow.compute, aggregate_function_name)
        aggregate_column_value = aggregate_function(raw_column_values).as_py()
        aggregate_column_name = f"{mapped_column_name}_{aggregate_function_name}"
        result[aggregate_column_name] = aggregate_column_value

    return pyarrow.Table.from_pylist([result])

class AggregateNode(BasePlanNode):
    def __init__(
        self, directives: QueryDirectives, statistics: QueryStatistics, **config
    ):
        super().__init__(directives=directives, statistics=statistics)

        self._aggregates = config.get("aggregates", [])
        self._groups = config.get("groups", [])

    @property
    def config(self):  # pragma: no cover
        return str(self._aggregates)

    @property
    def greedy(self):  # pragma: no cover
        return True

    @property
    def name(self):  # pragma: no cover
        return "Aggregation"

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, pyarrow.Table):
            data_pages = (data_pages,)

        if _is_count_star(self._aggregates, self._groups):
            yield from _count_star(data_pages)
            return

        # get all the columns anywhere in the groups or aggregates
        all_identifiers = get_all_identifiers(self._groups + self._aggregates)
        # join all the pages together, selecting only the columns we found above
        table = pyarrow.concat_tables(
            _project(data_pages.execute(), all_identifiers), promote=True
        )

        # Allow grouping by functions by evaluating them
        columns, self._groups, table = evaluate_and_append(self._groups, table)

        start_time = time.time_ns()
        group_by_columns = [columns.get_column_from_alias(group.value, only_one=True) for group in self._groups]

        column_map, aggs = _build_aggs(self._aggregates, columns)

        # we're not a group_by - either because the clause wasn't in the statement or
        # because we're grouping by all the available columns
        if len(group_by_columns) == 0:
            groups = _non_group_aggregates(self._aggregates, table, columns)
            del table
        else:
            groups = table.group_by(group_by_columns)
            groups = groups.aggregate(aggs)
        
        # name the aggregate fields
        for friendly_name, agg_name in column_map.items():
            columns.add_column(agg_name)
            columns.set_preferred_name(
                columns.get_column_from_alias(agg_name, only_one=True), friendly_name
            )
        groups = columns.apply(groups)

        self._statistics.time_aggregating += time.time_ns() - start_time

        yield groups
