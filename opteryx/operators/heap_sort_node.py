# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Heap Sort Node

This is a SQL Query Execution Plan Node.

This node orders a dataset, note that Heap Sort in this instance isn't the heap sort
algorithm, it is an approach where a heap of n items (the limit) is maintained as the
data passes through the operator. Because we are working with chunks, we build small
batches which we order and then discard the excess items.

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
from opteryx.models import QueryProperties

from . import BasePlanNode


class HeapSortNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.limit: int = parameters.get("limit", -1)

        self.mapped_order = []
        self.table = None

        for column, direction in self.order_by:
            try:
                self.mapped_order.append(
                    (
                        column.schema_column.identity,
                        direction,
                    )
                )
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                )

    @property
    def config(self):  # pragma: no cover
        return f"LIMIT = {self.limit}, ORDER = " + ", ".join(
            f"{i[0].value} {i[1][0:3].upper()}" for i in self.order_by
        )

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        if morsel == EOS:
            yield self.table
            yield EOS
            return

        if morsel.num_rows == 0:
            yield None
            return

        # Determine if any columns are string-based
        use_pyarrow_sort = any(
            pyarrow.types.is_string(morsel.column(column_name).type)
            or pyarrow.types.is_binary(morsel.column(column_name).type)
            for column_name, _ in self.mapped_order
        )

        use_decimal_sort = any(
            pyarrow.types.is_decimal(morsel.column(column_name).type)
            for column_name, _ in self.mapped_order
        )

        # strings are sorted faster using pyarrow, single columns faster using compute
        if len(self.mapped_order) == 1 and use_pyarrow_sort:
            column_name, sort_direction = self.mapped_order[0]
            column = morsel.column(column_name)
            if sort_direction == "ascending":
                sort_indices = pyarrow.compute.sort_indices(column)
            else:
                sort_indices = pyarrow.compute.sort_indices(column)[::-1]
            morsel = morsel.take(sort_indices[: self.limit])
        # strings are sorted faster using pyarrow
        elif use_pyarrow_sort:
            morsel = morsel.sort_by(self.mapped_order).slice(offset=0, length=self.limit)
        # single column sort using numpy
        elif len(self.mapped_order) == 1:
            # Single-column sort using argsort to take advantage of partially sorted data
            column_name, sort_direction = self.mapped_order[0]
            column = morsel.column(column_name).to_numpy()
            if use_decimal_sort:
                column = [decimal.Decimal("-Infinity") if x is None else x for x in column]
            if sort_direction == "ascending":
                sort_indices = numpy.argsort(column)
            else:
                sort_indices = numpy.argsort(column)[::-1]  # Reverse for descending

            # Slice the sorted table
            try:
                morsel = morsel.take(sort_indices[: self.limit])
            except Exception:
                mask = numpy.zeros(len(morsel), dtype=bool)
                mask[sort_indices[: self.limit]] = True
                morsel = morsel.filter(mask)

        # multi column sort using numpy
        else:
            # Multi-column sort using lexsort
            columns_for_sorting = []
            directions = []
            for column_name, sort_direction in self.mapped_order:
                column = morsel.column(column_name).to_numpy()
                columns_for_sorting.append(column)
                directions.append(1 if sort_direction == "ascending" else -1)

            sort_indices = numpy.lexsort(
                [col[::direction] for col, direction in zip(columns_for_sorting, directions)]
            )
            # Slice the sorted table
            morsel = morsel.take(sort_indices[: self.limit])

        if self.table:
            # Concatenate the accumulated table with the new morsel
            self.table = concat_tables([self.table, morsel], promote_options="permissive")
            self.table = self.table.sort_by(self.mapped_order).slice(offset=0, length=self.limit)
        else:
            self.table = morsel

        yield None
