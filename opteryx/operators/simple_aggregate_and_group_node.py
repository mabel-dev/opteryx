# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Simple Grouping Node

This is a SQL Query Execution Plan Node.

This is the grouping node, this specialized version only performs aggregations that result in,
and are collected as, a single value.
"""

import time

import numpy
import pyarrow
from orso.types import OrsoTypes

from opteryx import EOS
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_node import AGGREGATORS
from opteryx.operators.aggregate_node import build_aggregations
from opteryx.operators.aggregate_node import extract_evaluations
from opteryx.operators.aggregate_node import project

from . import BasePlanNode


def build_finalizer_aggregations(aggregators):
    column_map = {}
    aggs = []

    if not isinstance(aggregators, list):
        aggregators = [aggregators]

    for root in aggregators:
        for aggregator in get_all_nodes_of_type(root, select_nodes=(NodeType.AGGREGATOR,)):
            count_options = None

            field_name = aggregator.schema_column.identity
            if aggregator.value == "COUNT":
                function = AGGREGATORS["SUM"]
            else:
                function = AGGREGATORS[aggregator.value]
            # if the array agg is distinct, base off that function instead
            aggs.append((field_name, function, count_options))
            column_map[aggregator.schema_column.identity] = f"{field_name}_{function}".replace(
                "_hash_", "_"
            )

    return column_map, aggs


class SimpleAggregateAndGroupNode(BasePlanNode):
    SIMPLE_AGGREGATES = {"SUM", "MIN", "MAX", "COUNT"}

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.groups = list(parameters["groups"])
        self.aggregates = list(parameters["aggregates"])
        projection = list(parameters["projection"])

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

        self.finalizer_map, self.finalizer_aggregations = build_finalizer_aggregations(
            self.aggregates
        )

        self.buffer = []

    @property
    def config(self):  # pragma: no cover
        from opteryx.managers.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"

    @property
    def name(self):  # pragma: no cover
        return "Group By Simple"

    def execute(self, morsel: pyarrow.Table, **kwargs):
        internal_names = list(self.column_map.values()) + self.group_by_columns
        column_names = list(self.column_map.keys()) + self.group_by_columns

        if morsel == EOS:
            start = time.monotonic_ns()

            internal_names = list(self.finalizer_map.values()) + self.group_by_columns
            column_names = list(self.finalizer_map.keys()) + self.group_by_columns

            groups = pyarrow.concat_tables(self.buffer, promote_options="permissive")
            groups = groups.group_by(self.group_by_columns)
            groups = groups.aggregate(self.finalizer_aggregations)
            groups = groups.select(internal_names)
            groups = groups.rename_columns(column_names)

            self.statistics.time_groupby_finalize += time.monotonic_ns() - start
            yield groups
            yield EOS
            return

        morsel = project(morsel, self.all_identifiers)

        # Allow grouping by functions by evaluating them first
        if self.evaluatable_nodes:
            morsel = evaluate_and_append(self.evaluatable_nodes, morsel)

        morsel = evaluate_and_append(self.groups, morsel)

        # Add a "*" column, this is an int because when a bool it miscounts
        if "*" not in morsel.column_names:
            morsel = morsel.append_column(
                "*", [numpy.full(shape=morsel.num_rows, fill_value=1, dtype=numpy.int8)]
            )

        # use pyarrow to do phase 1 of the group by
        st = time.monotonic_ns()
        groups = morsel.group_by(self.group_by_columns)
        groups = groups.aggregate(self.aggregate_functions)
        groups = groups.select(internal_names)
        groups = groups.rename_columns(column_names)
        self.statistics.time_pregrouping += time.monotonic_ns() - st

        self.buffer.append(groups)

        yield None
