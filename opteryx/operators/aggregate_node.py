# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Aggregation Node

This is a SQL Query Execution Plan Node.

This node performs aggregates without performing groupings.
"""

import numpy
import pyarrow

from opteryx import EOS
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties

from . import BasePlanNode

COUNT_STAR: str = "COUNT(*)"

# use the aggregators from pyarrow
AGGREGATORS = {
    "ALL": "all",
    "ANY": "any",
    "APPROXIMATE_MEDIAN": "approximate_median",
    "ARRAY_AGG": "hash_list",
    "COUNT": "count",  # counts only non nulls
    "COUNT_DISTINCT": "count_distinct",
    "DISTINCT": "distinct",  # fated
    "LIST": "hash_list",  # fated
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
}


def _is_count_star(aggregates):
    """
    Is the SELECT clause `SELECT COUNT(*)` with no GROUP BY
    """
    if len(aggregates) != 1:
        return False
    if aggregates[0].value != "COUNT":
        return False
    return aggregates[0].parameters[0].node_type == NodeType.WILDCARD


def _count_star(morsel_promise, column_name):
    count = sum(morsel.num_rows for morsel in morsel_promise)
    table = pyarrow.Table.from_pylist([{column_name: count}])
    return table


def project(table: pyarrow.Table, column_names: list) -> pyarrow.Table:
    row_count = table.num_rows
    if len(column_names) > 0:
        return table.select(dict.fromkeys(column_names))
    else:
        # if we can't find the column, add a placeholder column
        return pyarrow.Table.from_pydict({"*": numpy.full(row_count, 1, dtype=numpy.int8)})


def build_aggregations(aggregators):
    column_map = {}
    aggs = []

    if not isinstance(aggregators, list):
        aggregators = [aggregators]

    for root in aggregators:
        for aggregator in get_all_nodes_of_type(root, select_nodes=(NodeType.AGGREGATOR,)):
            field_node = aggregator.parameters[0]
            count_options = None

            if field_node.node_type == NodeType.WILDCARD:
                field_name = "*"
                # count * counts nulls
                count_options = pyarrow.compute.CountOptions(mode="all")
            else:
                field_name = field_node.schema_column.identity
            function = AGGREGATORS[aggregator.value]
            # if the array agg is distinct, base off that function instead
            if aggregator.value == "ARRAY_AGG" and aggregator.distinct:
                function = "distinct"
            aggs.append((field_name, function, count_options))
            column_map[aggregator.schema_column.identity] = f"{field_name}_{function}".replace(
                "_hash_", "_"
            )

    return column_map, aggs


def _non_group_aggregates(aggregates, table):
    """
    If we're not doing a group by, we're just doing aggregations, the pyarrow
    functionality for aggregate doesn't work. So we do the calculation, it's
    relatively straightforward as it's the entire table we're summarizing.
    """

    result = {}

    for aggregate in aggregates:
        if aggregate.node_type in (NodeType.AGGREGATOR,):
            column_node = aggregate.parameters[0]
            if column_node.node_type == NodeType.LITERAL:
                raw_column_values = numpy.full(table.num_rows, column_node.value)
            elif (
                aggregate.value == "COUNT"
                and aggregate.parameters[0].node_type == NodeType.WILDCARD
            ):
                result[aggregate.schema_column.identity] = table.num_rows
                continue
            else:
                raw_column_values = table[column_node.schema_column.identity].to_numpy()
            aggregate_function_name = AGGREGATORS[aggregate.value]
            # this maps a string which is the function name to that function on the
            # pyarrow.compute module
            if not hasattr(pyarrow.compute, aggregate_function_name):
                raise UnsupportedSyntaxError(
                    f"Aggregate `{aggregate.value}` can only be used with GROUP BY"
                )
            aggregate_function = getattr(pyarrow.compute, aggregate_function_name)
            aggregate_column_value = aggregate_function(raw_column_values).as_py()
            result[aggregate.schema_column.identity] = aggregate_column_value

    return pyarrow.Table.from_pylist([result])


def extract_evaluations(aggregates):
    # extract any inner evaluations, like the IIF in SUM(IIF(x, 1, 0))

    all_evaluatable_nodes = get_all_nodes_of_type(
        aggregates,
        select_nodes=(
            NodeType.FUNCTION,
            NodeType.BINARY_OPERATOR,
            NodeType.COMPARISON_OPERATOR,
            NodeType.LITERAL,
        ),
    )

    evaluatable_nodes = []
    for node in all_evaluatable_nodes:
        aggregators = get_all_nodes_of_type(node, select_nodes=(NodeType.AGGREGATOR,))
        if len(aggregators) == 0:
            evaluatable_nodes.append(node)

    literal_count = len([n for n in evaluatable_nodes if n.node_type == NodeType.LITERAL])
    if literal_count > 0 and literal_count < len(evaluatable_nodes):
        evaluatable_nodes = [n for n in evaluatable_nodes if n.node_type != NodeType.LITERAL]

    return evaluatable_nodes


class AggregateNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.aggregates = parameters.get("aggregates", [])

        # get all the columns anywhere in the aggregates
        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(self.aggregates, select_nodes=(NodeType.IDENTIFIER,))
        ]
        self.all_identifiers = list(dict.fromkeys(all_identifiers))

        # Get any functions we need to execute before aggregating
        self.evaluatable_nodes = extract_evaluations(self.aggregates)

        self.column_map, self.aggregate_functions = build_aggregations(self.aggregates)

        self.buffer = []

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return str(self.aggregates)

    @property
    def name(self):  # pragma: no cover
        return "Aggregation"

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        if morsel == EOS:
            if _is_count_star(self.aggregates):
                yield _count_star(
                    morsel_promise=self.buffer,
                    column_name=self.aggregates[0].schema_column.identity,
                )
                yield EOS
                return

            # merge all the morsels together into one table, selecting only the columns
            # we're pretty sure we're going to use - this will fail for datasets
            # larger than memory until we implement some form of partitioning
            table = pyarrow.concat_tables(self.buffer, promote_options="none")

            # Allow grouping by functions by evaluating them first
            if self.evaluatable_nodes:
                table = evaluate_and_append(self.evaluatable_nodes, table)

            # Add a "*" column, this is an int because when a bool it miscounts
            if "*" not in table.column_names:
                table = table.append_column(
                    "*", [numpy.full(shape=table.num_rows, fill_value=1, dtype=numpy.int8)]
                )

            # we're not a group_by - we're aggregating without grouping
            aggregates = _non_group_aggregates(self.aggregates, table)
            del table

            # name the aggregate fields and add them to the Columns data
            aggregates = aggregates.select(list(self.column_map.keys()))

            yield aggregates
            yield EOS
            return

        self.buffer.append(project(morsel, self.all_identifiers))
        yield None
