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

import pyarrow
import pyarrow.compute as pc
from pyarrow import concat_tables

from opteryx import EOS
from opteryx.exceptions import ColumnNotFoundError
from opteryx.models import QueryProperties

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
                ) from cnfe
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
        morsel = self.ensure_arrow_table(morsel)

        _ = kwargs  # kwargs are part of the execution contract
        if morsel is EOS:
            if self.table is None:
                yield EOS
                return

            if (self.limit is None or self.limit <= 0) and self.mapped_order:
                self.table = self.table.sort_by(self.mapped_order)
            elif self.limit and self.limit > 0 and self.mapped_order:
                # Ensure sorted output for the final chunk
                self.table = self.table.sort_by(self.mapped_order)
                if self.table.num_rows > self.limit:
                    self.table = self.table.slice(0, self.limit)

            yield self.table
            yield EOS
            return

        if morsel.num_rows == 0:
            yield None
            return

        if self.limit and self.limit > 0:
            morsel = self._top_n(morsel)
            if self.table is None:
                self.table = morsel
            else:
                combined = concat_tables([self.table, morsel], promote_options="permissive")
                self.table = self._top_n(combined)
        else:
            if self.table is None:
                self.table = morsel
            else:
                self.table = concat_tables([self.table, morsel], promote_options="permissive")

        yield None

    def _top_n(self, table: pyarrow.Table) -> pyarrow.Table:
        if self.limit is None or self.limit <= 0:
            return table

        k = min(self.limit, table.num_rows)
        if k == 0:
            return table.slice(0, 0)

        if not self.mapped_order:
            return table.slice(0, k)

        if k < table.num_rows:
            try:
                indices = pc.select_k_unstable(  # type: ignore[attr-defined]
                    table,
                    k=k,
                    sort_keys=self.mapped_order,
                )
            except AttributeError:
                limited = table.sort_by(self.mapped_order).slice(0, k)
            except (TypeError, ValueError, NotImplementedError):
                limited = table.sort_by(self.mapped_order).slice(0, k)
            else:
                limited = table.take(indices)
                return limited.sort_by(self.mapped_order)

            return limited

        sorted_table = table.sort_by(self.mapped_order)
        return sorted_table.slice(0, k)
