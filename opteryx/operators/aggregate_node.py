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

This is built around the pyarrow table grouping functionality.
"""
import time

from typing import Iterable

import pyarrow

from opteryx.exceptions import SqlError
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import format_expression
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.models.columns import Columns

COUNT_STAR: str = "COUNT(*)"

# use the aggregators from pyarrow
AGGREGATORS = {
    "ALL": "all",
    "ANY": "any",
    "APPROXIMATE_MEDIAN": "approximate_median",
    "COUNT": "count",  # counts only non nulls
    "COUNT_DISTINCT": "count_distinct",
    "DISTINCT": "distinct",
    "LIST": "hash_list",
    "MAX": "max",
    "MAXIMUM": "max",  # alias
    "MEAN": "mean",
    #    "MODE": "mode",
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
    #    "QUANTILES": "tdigest",
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

    if not isinstance(aggregators, list):
        aggregators = [aggregators]

    for root in aggregators:

        for aggregator in get_all_nodes_of_type(
            root, select_nodes=(NodeType.AGGREGATOR,)
        ):

            if aggregator.token_type == NodeType.AGGREGATOR:
                field_node = aggregator.parameters[0]
                display_name = format_expression(field_node)
                exists = columns.get_column_from_alias(display_name)

                if field_node.token_type == NodeType.WILDCARD:
                    display_field = "*"
                    field_name = columns.preferred_column_names[0][0]
                elif field_node.token_type == NodeType.IDENTIFIER:
                    display_field = field_node.value
                    field_name = columns.get_column_from_alias(
                        field_node.value, only_one=True
                    )
                elif len(exists) > 0:
                    display_field = exists[0]
                    field_name = exists[0]
                else:
                    display_name = format_expression(field_node)
                    raise SqlError(
                        f"Invalid identifier provided in aggregator function `{display_name}`"
                    )
                function = AGGREGATORS.get(aggregator.value)
                aggs.append(
                    (
                        field_name,
                        function,
                    )
                )
                column_map[
                    f"{aggregator.value.upper()}({display_field})"
                ] = f"{field_name}_{function}".replace("_hash_", "_")

    return column_map, list(set(aggs))


def _non_group_aggregates(aggregates, table, columns):
    """
    If we're not doing a group by, we're just doing aggregations, the pyarrow
    functionality for aggregate doesn't work. So we do the calculation, it's
    relatively straightforward as it's the entire table we're summarizing.
    """

    result = {}

    for aggregate in aggregates:

        if aggregate.token_type == NodeType.AGGREGATOR:

            column_name = format_expression(aggregate.parameters[0])
            mapped_column_name = columns.get_column_from_alias(
                column_name, only_one=True
            )
            raw_column_values = table[mapped_column_name].to_numpy()
            aggregate_function_name = AGGREGATORS[aggregate.value]
            # this maps a string which is the function name to that function on the
            # pyarrow.compute module
            aggregate_function = getattr(pyarrow.compute, aggregate_function_name)
            aggregate_column_value = aggregate_function(raw_column_values).as_py()
            aggregate_column_name = f"{mapped_column_name}_{aggregate_function_name}"
            result[aggregate_column_name] = aggregate_column_value

    #        else:

    #            raise SqlError(
    #                "Cannot mix aggregate and non aggregate columns in SELECT statement."
    #            )

    return pyarrow.Table.from_pylist([result])


def _extract_functions(aggregates):

    # extract any inner evaluations, like the IIF in SUM(IIF(x, 1, 0))

    all_evaluatable_nodes = get_all_nodes_of_type(
        aggregates,
        select_nodes=(
            NodeType.FUNCTION,
            NodeType.BINARY_OPERATOR,
            NodeType.COMPARISON_OPERATOR,
        ),
    )

    evaluatable_nodes = []
    for node in all_evaluatable_nodes:
        aggregators = get_all_nodes_of_type(node, select_nodes=(NodeType.AGGREGATOR,))
        if len(aggregators) == 0:
            evaluatable_nodes.append(node)

    return evaluatable_nodes


class AggregateNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

        self._aggregates = config.get("aggregates", [])

        self._groups = []
        for group in config.get("groups", []):
            # skip nulls
            if group is None:
                continue
            # handle numeric indexes - GROUP BY 1
            if group.token_type == NodeType.LITERAL_NUMERIC:
                # references are natural numbers, so -1 for zero-based
                group = self._aggregates[int(group.value) - 1]
                if group.token_type not in (NodeType.IDENTIFIER, NodeType.FUNCTION):
                    raise SqlError(
                        "When using a positional reference, GROUP BY must be by an IDENTIFIER or FUNCTION only"
                    )
            self._groups.append(group)

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
        all_identifiers = set(
            [
                node.value
                for node in get_all_nodes_of_type(
                    self._groups + self._aggregates, select_nodes=(NodeType.IDENTIFIER,)
                )
            ]
        )
        # join all the pages together, selecting only the columns we found above
        table = pyarrow.concat_tables(
            _project(data_pages.execute(), all_identifiers), promote=True
        )

        # get any functions we need to execute before aggregating
        evaluatable_nodes = _extract_functions(self._aggregates)

        # Allow grouping by functions by evaluating them
        start_time = time.time_ns()
        columns, _, table = evaluate_and_append(evaluatable_nodes, table)
        columns, self._groups, table = evaluate_and_append(self._groups, table)
        self._statistics.time_evaluating += time.time_ns() - start_time

        start_time = time.time_ns()
        group_by_columns = [
            columns.get_column_from_alias(group.value, only_one=True)
            for group in self._groups
        ]

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
            column_name = columns.get_column_from_alias(agg_name, only_one=True)
            columns.set_preferred_name(column_name, friendly_name)
            # if we have an alias for this column, add it to the metadata
            aliases = [
                agg.alias
                for agg in self._aggregates
                if friendly_name == format_expression(agg)
            ]
            for alias in aliases:
                if alias:
                    columns.add_alias(column_name, alias)
        groups = columns.apply(groups)

        self._statistics.time_aggregating += time.time_ns() - start_time

        yield groups
