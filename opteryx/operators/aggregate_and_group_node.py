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

import time

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

        aggregator_nodes = get_all_nodes_of_type(
            self.aggregates, select_nodes=(NodeType.AGGREGATOR,)
        )
        self._aggregator_metadata = list(zip(aggregator_nodes, self.aggregate_functions))
        (
            self._partial_merge_aggs,
            self._final_partial_column_map,
            self._buffer_partial_column_map,
        ) = self._build_partial_merge_plan()

        self.buffer = []
        self.max_buffer_size = (
            250  # Buffer size before partial aggregation (kept for future parallelization)
        )
        self._partial_aggregated = False  # Track if we've done a partial aggregation
        self._disable_partial_agg = False  # Can disable if partial agg isn't helping

    def _build_partial_merge_plan(self):
        merge_aggs = []
        final_column_map = {}
        buffer_column_map = {}
        for aggregator_node, (field_name, function, _count_options) in self._aggregator_metadata:
            alias = aggregator_node.schema_column.identity
            source_column = self.column_map.get(alias)
            if function == "count":
                source_column = f"{field_name}_count"
                merge_aggs.append((source_column, "sum", None))
                final_column_map[alias] = f"{source_column}_sum"
                buffer_column_map[source_column] = f"{source_column}_sum"
            elif function in ("sum", "max", "min", "hash_one", "all", "any"):
                merge_aggs.append((source_column, function, None))
                renamed = f"{source_column}_{function}".replace("_hash_", "_")
                final_column_map[alias] = renamed
                buffer_column_map[source_column] = renamed
            elif function == "mean":
                merge_aggs.append((source_column, "hash_one", None))
                renamed = f"{source_column}_one"
                final_column_map[alias] = renamed
                buffer_column_map[source_column] = renamed
            elif function == "hash_list":
                merge_aggs.append((source_column, "hash_list", None))
                renamed = f"{source_column}_list"
                final_column_map[alias] = renamed
                buffer_column_map[source_column] = renamed
            else:
                merge_aggs.append((source_column, "hash_one", None))
                renamed = f"{source_column}_one"
                final_column_map[alias] = renamed
                buffer_column_map[source_column] = renamed

        return merge_aggs, final_column_map, buffer_column_map

    def _reaggregate_partial_results(self, table, *, final: bool):
        if not self._partial_merge_aggs or table.num_rows == 0:
            return table

        groups = table.group_by(self.group_by_columns)
        groups = groups.aggregate(self._partial_merge_aggs)

        column_map = self._final_partial_column_map if final else self._buffer_partial_column_map
        if column_map:
            groups = groups.select(list(column_map.values()) + self.group_by_columns)
            groups = groups.rename_columns(list(column_map.keys()) + self.group_by_columns)

        return groups

    def _merge_partial_buffer(self):
        table = pyarrow.concat_tables(
            self.buffer,
            promote_options="permissive",
        )
        return self._reaggregate_partial_results(table, final=False)

    @property
    def config(self):  # pragma: no cover
        from opteryx.managers.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"

    @property
    def name(self):  # pragma: no cover
        return "Group By"

    def execute(self, morsel: pyarrow.Table, **kwargs):
        morsel = self.ensure_arrow_table(morsel)

        if morsel == EOS:
            if not self.buffer:
                yield EOS
                return

            # Do final aggregation if we have buffered data
            table = pyarrow.concat_tables(
                self.buffer,
                promote_options="permissive",
            )

            # If we've done partial aggregations, the aggregate functions need adjusting
            # because columns like "*" have been renamed to "*_count"
            if self._partial_aggregated:
                groups = self._reaggregate_partial_results(table, final=True)
            else:
                groups = table.group_by(self.group_by_columns)
                groups = groups.aggregate(self.aggregate_functions)

                # project to the desired column names from the pyarrow names
                groups = groups.select(list(self.column_map.values()) + self.group_by_columns)
                groups = groups.rename_columns(list(self.column_map.keys()) + self.group_by_columns)

            # do the secondary activities for ARRAY_AGG (order and limit)
            array_agg_nodes = [
                node
                for node in get_all_nodes_of_type(
                    self.aggregates, select_nodes=(NodeType.AGGREGATOR,)
                )
                if node.value == "ARRAY_AGG" and (node.order or node.limit)
            ]

            if array_agg_nodes:
                # Process all ARRAY_AGG columns that need ordering/limiting
                arrays_to_update = {}
                field_defs = {}

                for node in array_agg_nodes:
                    column_name = node.schema_column.identity

                    # Store field definition before we drop the column
                    field_defs[column_name] = groups.field(column_name)

                    # Extract and process the data
                    column_data = groups.column(column_name).to_pylist()

                    # Apply ordering if specified
                    if node.order:
                        column_data = [
                            sorted(c, reverse=bool(node.order[0][1])) for c in column_data
                        ]

                    # Apply limit if specified
                    if node.limit:
                        column_data = [c[: node.limit] for c in column_data]

                    arrays_to_update[column_name] = column_data

                # Drop all columns we're updating
                columns_to_drop = list(arrays_to_update.keys())
                groups = groups.drop(columns_to_drop)

                # Append all updated columns back
                for column_name, column_data in arrays_to_update.items():
                    groups = groups.append_column(field_defs[column_name], [column_data])

            num_rows = groups.num_rows
            for start in range(0, num_rows, CHUNK_SIZE):
                yield groups.slice(start, min(CHUNK_SIZE, num_rows - start))

            yield EOS
            return

        morsel = project(morsel, self.all_identifiers)
        # Add a "*" column, this is an int because when a bool it miscounts
        # FIX: Use int8 as the comment states (bool can miscount)
        if "*" not in morsel.column_names:
            morsel = morsel.append_column(
                "*", [numpy.ones(shape=morsel.num_rows, dtype=numpy.int8)]
            )
        eval_start = time.monotonic_ns()
        if self.evaluatable_nodes:
            morsel = evaluate_and_append(self.evaluatable_nodes, morsel)
        morsel = evaluate_and_append(self.groups, morsel)
        self.statistics.time_group_by_evaluations += time.monotonic_ns() - eval_start

        self.buffer.append(morsel)

        # If we've already partially aggregated, convert incoming morsels to aggregated chunks
        if self._partial_aggregated and morsel is not None and morsel != EOS:
            group_start = time.monotonic_ns()
            morsel = morsel.group_by(self.group_by_columns)
            morsel = morsel.aggregate(self.aggregate_functions)
            self.statistics.time_pregrouping += time.monotonic_ns() - group_start
            self.buffer[-1] = morsel

        # If buffer is full, do partial aggregation
        # BUT: Skip partial aggregation if it's not reducing data effectively
        if (
            not self._partial_aggregated
            and len(self.buffer) >= self.max_buffer_size
            and not self._disable_partial_agg
        ):
            table = pyarrow.concat_tables(
                self.buffer,
                promote_options="permissive",
            )

            groups = table.group_by(self.group_by_columns)
            groups = groups.aggregate(self.aggregate_functions)

            # Check if partial aggregation is effective
            # If we're not reducing the row count significantly, stop doing partial aggs
            reduction_ratio = groups.num_rows / table.num_rows if table.num_rows > 0 else 1
            if reduction_ratio > 0.75:  # Kept more than 75% of rows - high cardinality!
                # Partial aggregation isn't helping, disable it and keep buffering
                self._disable_partial_agg = True
                # Don't replace buffer with partial result, keep accumulating
            else:
                # Good reduction, keep using partial aggregation
                self.buffer = [groups]  # Replace buffer with partial result
                self._partial_aggregated = True  # Mark that we've done a partial aggregation
        elif self._partial_aggregated and len(self.buffer) >= self.max_buffer_size:
            self.buffer = [self._merge_partial_buffer()]

        yield None
