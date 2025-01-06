# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Grouping Node

This is a SQL Query Execution Plan Node.

This is the grouping node, it is always followed by the aggregation node, but
the aggregation node doesn't need the grouping node.


"""

import numpy
import pyarrow
from orso.types import OrsoTypes

from opteryx import EOS
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_node import build_aggregations
from opteryx.operators.aggregate_node import extract_evaluations
from opteryx.operators.aggregate_node import project

from . import BasePlanNode

CHUNK_SIZE = 65536


class AggregateAndGroupNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.groups = list(parameters["groups"])
        self.aggregates = list(parameters["aggregates"])
        projection = list(parameters["projection"])

        # we're going to preload some of the evaluation

        # Replace offset based GROUP BYs with their column
        self.groups = [
            (
                group
                if not (group.node_type == NodeType.LITERAL and group.type == OrsoTypes.INTEGER)
                else projection[group.value - 1]
            )
            for group in self.groups
        ]

        # get all the columns anywhere in the groups or aggregates
        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(
                self.groups + self.aggregates, select_nodes=(NodeType.IDENTIFIER,)
            )
        ]
        self.all_identifiers = list(dict.fromkeys(all_identifiers))

        # Get any functions we need to execute before aggregating
        self.evaluatable_nodes = extract_evaluations(self.aggregates)

        # get the aggregated groupings and functions
        self.group_by_columns = list({node.schema_column.identity for node in self.groups})
        self.column_map, self.aggregate_functions = build_aggregations(self.aggregates)

        self.buffer = []

    @property
    def config(self):  # pragma: no cover
        from opteryx.managers.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"

    @property
    def name(self):  # pragma: no cover
        return "Group By"

    def execute(self, morsel: pyarrow.Table, **kwargs):
        if morsel == EOS:
            # merge all the morsels together into one table, selecting only the columns
            # we're pretty sure we're going to use - this will fail for datasets
            # larger than memory
            table = pyarrow.concat_tables(
                self.buffer,
                promote_options="permissive",
            )

            # do the group by and aggregates
            table = table.combine_chunks()
            groups = table.group_by(self.group_by_columns)
            groups = groups.aggregate(self.aggregate_functions)

            # do the secondary activities for ARRAY_AGG
            for node in get_all_nodes_of_type(self.aggregates, select_nodes=(NodeType.AGGREGATOR,)):
                if node.value == "ARRAY_AGG" and node.order or node.limit:
                    # rip the column out of the table
                    column_name = self.column_map[node.schema_column.identity]
                    column_def = groups.field(column_name)  # this is used
                    column = groups.column(column_name).to_pylist()
                    groups = groups.drop([column_name])
                    if node.order:
                        column = [sorted(c, reverse=bool(node.order[0][1])) for c in column]
                    if node.limit:
                        column = [c[: node.limit] for c in column]
                    # put the new column into the table
                    groups = groups.append_column(column_def, [column])

            # project to the desired column names from the pyarrow names
            groups = groups.select(list(self.column_map.values()) + self.group_by_columns)
            groups = groups.rename_columns(list(self.column_map.keys()) + self.group_by_columns)

            num_rows = groups.num_rows
            for start in range(0, num_rows, CHUNK_SIZE):
                yield groups.slice(start, min(CHUNK_SIZE, num_rows - start))

            yield EOS
            return

        morsel = project(morsel, self.all_identifiers)
        # Add a "*" column, this is an int because when a bool it miscounts
        if "*" not in morsel.column_names:
            morsel = morsel.append_column(
                "*", [numpy.ones(shape=morsel.num_rows, dtype=numpy.bool_)]
            )
        if self.evaluatable_nodes:
            morsel = evaluate_and_append(self.evaluatable_nodes, morsel)
        morsel = evaluate_and_append(self.groups, morsel)

        self.buffer.append(morsel)
        yield None
