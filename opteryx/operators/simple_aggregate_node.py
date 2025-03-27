# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Simple Aggregation Node

This is a SQL Query Execution Plan Node.

This node performs aggregates without performing groupings, this is a specialized version
which focuses on building aggregates which don't require seeing the entire dataset at a time.

We avoid doing some work by not creating entire columns of data where possible.
"""

import pyarrow

from opteryx import EOS
from opteryx.compiled.aggregations.count_distinct import count_distinct
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_node import extract_evaluations
from opteryx.third_party.abseil.containers import FlatHashSet

from . import BasePlanNode


class SimpleAggregateCollector:
    def __init__(
        self, aggregate_type, schema_column, count_nulls=False, duplicate_treatment="IGNORE"
    ):
        self.aggregate_type = aggregate_type
        self.current_value = None
        self.count_nulls = count_nulls if aggregate_type == "COUNT" else False
        self.duplicate_treatment = duplicate_treatment
        self.counter = 0
        self.schema_column = schema_column
        self.column_type = schema_column.type
        self.always_count = aggregate_type in ("COUNT", "AVG")

    def collect(self, values):
        if self.always_count and self.count_nulls:
            self.counter += pyarrow.compute.count(values).as_py()
        elif self.always_count:
            self.counter += pyarrow.compute.count(values, mode="only_valid").as_py()

        if self.current_value is None:
            if self.aggregate_type in ("SUM", "AVG"):
                self.current_value = pyarrow.compute.sum(values).as_py()
            elif self.aggregate_type == "MIN":
                self.current_value = pyarrow.compute.min(values).as_py()
            elif self.aggregate_type == "MAX":
                self.current_value = pyarrow.compute.max(values).as_py()
            elif self.aggregate_type == "COUNT" and self.duplicate_treatment == "Distinct":
                self.current_value = count_distinct(values, FlatHashSet())
            elif self.aggregate_type == "HISTOGRAM":
                from opteryx.third_party.maki_nage.distogram import Distogram

                self.current_value = Distogram()
                self.current_value.bulkload(values.to_numpy(False))
            elif self.aggregate_type != "COUNT":
                raise ValueError(f"Unsupported aggregate type: {self.aggregate_type}")
        else:
            if self.aggregate_type in ("SUM", "AVG"):
                new_value = pyarrow.compute.sum(values).as_py()
                if new_value is not None:
                    self.current_value += new_value
            elif self.aggregate_type == "MIN":
                new_value = pyarrow.compute.min(values).as_py()
                if new_value is not None:
                    self.current_value = min(self.current_value, new_value)
            elif self.aggregate_type == "MAX":
                new_value = pyarrow.compute.max(values).as_py()
                if new_value is not None:
                    self.current_value = max(self.current_value, new_value)
            elif self.aggregate_type == "HISTOGRAM":
                self.current_value.bulkload(values.to_numpy(False))
            elif self.aggregate_type == "COUNT" and self.duplicate_treatment == "Distinct":
                self.current_value = count_distinct(values, self.current_value)
            elif self.aggregate_type != "COUNT":
                raise ValueError(f"Unsupported aggregate type: {self.aggregate_type}")

    def collect_literal(self, literal, count):
        self.counter += count

        if self.current_value is None:
            if self.aggregate_type in ("SUM", "AVG"):
                self.current_value = literal * count
            elif self.aggregate_type == "MIN" or self.aggregate_type == "MAX":
                self.current_value = literal
            elif self.aggregate_type != "COUNT":
                raise ValueError(f"Unsupported aggregate type: {self.aggregate_type}")
        else:
            if self.aggregate_type in ("SUM", "AVG"):
                self.current_value += literal * count
            elif self.aggregate_type == "MIN":
                self.current_value = min(self.current_value, literal)
            elif self.aggregate_type == "MAX":
                self.current_value = max(self.current_value, literal)
            elif self.aggregate_type != "COUNT":
                raise ValueError(f"Unsupported aggregate type: {self.aggregate_type}")

    def get_result(self):
        if self.aggregate_type == "AVG":
            if self.counter == 0 or self.current_value is None:
                return None
            return self.current_value / self.counter
        if self.aggregate_type == "COUNT" and self.duplicate_treatment == "Distinct":
            return self.current_value.items()
        if self.aggregate_type == "COUNT":
            return self.counter
        if self.aggregate_type == "HISTOGRAM":
            from opteryx.third_party.maki_nage.distogram import histogram

            return histogram(self.current_value)
        return self.current_value


class SimpleAggregateNode(BasePlanNode):
    SIMPLE_AGGREGATES = {"AVG", "COUNT", "COUNT_DISTINCT", "HISTOGRAM", "MAX", "MIN", "SUM"}

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.aggregates = parameters.get("aggregates", [])

        # Get any functions we need to execute before aggregating
        self.evaluatable_nodes = extract_evaluations(self.aggregates)

        # Create collectors for each aggregate
        self.accumulator = {}
        for aggregate in self.aggregates:
            aggregate_type = aggregate.value
            final_column_id = aggregate.schema_column.identity

            self.accumulator[final_column_id] = SimpleAggregateCollector(
                aggregate_type,
                aggregate.parameters[0].schema_column,
                duplicate_treatment=aggregate.duplicate_treatment,
            )

    @property
    def config(self):  # pragma: no cover
        return str(self.aggregates)

    @property
    def name(self):  # pragma: no cover
        return "Aggregation Simple"

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        if morsel == EOS:
            names = []
            values = []
            for k, v in self.accumulator.items():
                names.append(k)
                values.append([v.get_result()])
            yield pyarrow.Table.from_arrays(values, names=names)
            yield EOS
            return

        # Allow grouping by functions by evaluating them first
        if self.evaluatable_nodes:
            morsel = evaluate_and_append(self.evaluatable_nodes, morsel)

        for aggregate in self.aggregates:
            if aggregate.node_type in (NodeType.AGGREGATOR,):
                column_node = aggregate.parameters[0]

                if column_node.node_type == NodeType.LITERAL:
                    self.accumulator[aggregate.schema_column.identity].collect_literal(
                        column_node.value, morsel.num_rows
                    )
                elif column_node.node_type == NodeType.WILDCARD:
                    if "$COUNT(*)" in morsel.column_names and morsel.num_rows > 0:
                        self.accumulator[aggregate.schema_column.identity].collect_literal(
                            1, morsel["$COUNT(*)"][0].as_py()
                        )
                    else:
                        self.accumulator[aggregate.schema_column.identity].collect_literal(
                            1, morsel.num_rows
                        )
                else:
                    raw_column_values = morsel[column_node.schema_column.identity]
                    self.accumulator[aggregate.schema_column.identity].collect(raw_column_values)
