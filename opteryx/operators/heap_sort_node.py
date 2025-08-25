# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Heap Sort Node (Top-N Sort)

This is a SQL Query Execution Plan Node.

This query plan node maintains a sorted table of the top-N records seen so far
based on the provided ORDER BY clause. Despite the name, this is not a Heap Sort
algorithm, but an incremental Top-N sorter that works chunk-wise for efficiency.

This is faster, particularly when working with large datasets even though we're now
sorting smaller chunks over and over again.
"""

import decimal

import numpy
import pyarrow
import pyarrow.compute
from pyarrow import concat_tables

from opteryx import EOS
from opteryx.exceptions import ColumnNotFoundError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.managers.expression.binary_operators import BINARY_OPERATORS
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.planner import build_literal_node

from . import BasePlanNode


class HeapSortNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.limit = parameters.get("limit", -1)

        self.mapped_order = []
        for column, direction in self.order_by:
            try:
                self.mapped_order.append((column.schema_column.identity, direction))
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns from `SELECT`. {cnfe}"
                )

        self.table = None

    @property
    def config(self):  # pragma: no cover
        order = ", ".join(
            f"{col.schema_column.name} {dir[:3].upper()}" for col, dir in self.order_by
        )
        return f"LIMIT = {self.limit}, ORDER = {order}"

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    def execute(self, morsel: pyarrow.Table, **kwargs):
        if morsel is EOS:
            yield self.table
            yield EOS
            return

        if morsel.num_rows == 0:
            yield None
            return

        morsel = self._sort_and_slice(morsel)

        if self.table:
            self.table = concat_tables([self.table, morsel], promote_options="permissive")
            self.table = self.table.sort_by(self.mapped_order).slice(0, self.limit)
        else:
            self.table = morsel

        yield None

    def _sort_and_slice(self, table: pyarrow.Table) -> pyarrow.Table:
        if not self.mapped_order:
            return table.slice(0, self.limit)

        # Detect column types
        use_arrow = any(
            pyarrow.types.is_string(table.column(col).type)
            or pyarrow.types.is_binary(table.column(col).type)
            for col, _ in self.mapped_order
        )
        use_decimal = any(
            pyarrow.types.is_decimal(table.column(col).type) for col, _ in self.mapped_order
        )

        # Case 1: Single column, Arrow sort
        if len(self.mapped_order) == 1:
            col_name, direction = self.mapped_order[0]
            column = table.column(col_name)

            if use_arrow:
                if direction == "ascending":
                    indices = pyarrow.compute.sort_indices(column)
                else:
                    indices = pyarrow.compute.sort_indices(column)[::-1]
                return table.take(indices.slice(0, self.limit))

            np_column = column.to_numpy()
            if use_decimal:
                np_column = [decimal.Decimal("-Infinity") if v is None else v for v in np_column]

            indices = numpy.argsort(np_column)
            if direction == "descending":
                indices = indices[::-1]

            return self._safe_take(table, indices)

        # Case 2: Multi-column sort
        if use_arrow:
            return table.sort_by(self.mapped_order).slice(0, self.limit)

        # Fallback to numpy.lexsort
        sort_arrays = []
        for col_name, direction in self.mapped_order:
            arr = table.column(col_name).to_numpy()
            if direction == "descending":
                arr = arr[::-1]
            sort_arrays.append(arr)

        indices = numpy.lexsort(sort_arrays[::-1])
        return self._safe_take(table, indices)

    def _safe_take(self, table: pyarrow.Table, indices: numpy.ndarray) -> pyarrow.Table:
        try:
            return table.take(indices[: self.limit])
        except Exception:
            mask = numpy.zeros(len(table), dtype=bool)
            mask[indices[: self.limit]] = True
            return table.filter(mask)
