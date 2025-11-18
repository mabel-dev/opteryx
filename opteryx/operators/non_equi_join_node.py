# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Non-Equi Join Node

This is a SQL Query Execution Plan Node.

This implements non-equi joins (comparisons other than equality) using a nested loop
algorithm with draken for columnar operations. This is an unoptimized implementation
focused on correctness first.

Supported comparisons:
- NOT EQUAL (!=)
- GREATER THAN (>)
- GREATER THAN OR EQUAL (>=)
- LESS THAN (<)
- LESS THAN OR EQUAL (<=)
"""

import numpy
import pyarrow
from pyarrow import Table

from opteryx import EOS
from opteryx.compiled.joins import non_equi_nested_loop_join
from opteryx.draken import Morsel
from opteryx.draken import align_tables
from opteryx.models import QueryProperties

from . import JoinNode

# from opteryx.utils.arrow import align_tables


class NonEquiJoinNode(JoinNode):
    """
    Implements non-equi joins using nested loop algorithm with draken.
    """

    join_type = "non equi"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_column = parameters.get("on").get("left").schema_column.identity
        self.right_column = parameters.get("on").get("right").schema_column.identity
        self.comparison_op = parameters.get("on").get("value")

        self.left_relation = None
        self.left_morsel = None
        self.left_buffer = []

        # Validate comparison operator
        valid_ops = [
            "NotEq",
            "Gt",
            "GtEq",
            "Lt",
            "LtEq",
        ]
        if self.comparison_op not in valid_ops:
            raise ValueError(f"Unsupported comparison operator: {self.comparison_op}")

    @property
    def name(self):  # pragma: no cover
        return "Non-Equi Join"

    @property
    def config(self):  # pragma: no cover
        op_symbols = {
            "not_equals": "!=",
            "greater_than": ">",
            "greater_than_or_equals": ">=",
            "less_than": "<",
            "less_than_or_equals": "<=",
        }
        op_symbol = op_symbols.get(self.comparison_op, self.comparison_op)
        return f"{self.left_column} {op_symbol} {self.right_column}"

    def execute(self, morsel: Table, join_leg: str) -> Table:
        morsel = self.ensure_arrow_table(morsel)

        if join_leg == "left":
            if morsel == EOS:
                self.left_relation = pyarrow.concat_tables(self.left_buffer, promote_options="none")
                self.left_buffer.clear()
                self.left_morsel = Morsel.from_arrow(self.left_relation)
            else:
                self.left_buffer.append(morsel)
            yield None
            return

        if join_leg == "right":
            if morsel == EOS:
                yield EOS
                return

            if morsel is EOS:
                left_indexes = numpy.array([], dtype=numpy.int32)
                right_indexes = numpy.array([], dtype=numpy.int32)
            else:
                right_morsel = Morsel.from_arrow(morsel)
                left_indexes, right_indexes = non_equi_nested_loop_join(
                    self.left_morsel,
                    right_morsel,
                    self.left_column,
                    self.right_column,
                    self.comparison_op,
                )
                # Convert to int32 for align_tables
                left_indexes = numpy.asarray(left_indexes, dtype=numpy.int32)
                right_indexes = numpy.asarray(right_indexes, dtype=numpy.int32)

            result_morsel = align_tables(
                self.left_morsel, right_morsel, left_indexes, right_indexes
            )
            yield result_morsel
