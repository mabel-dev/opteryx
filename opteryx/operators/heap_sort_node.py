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
Heap Sort Node

This is a SQL Query Execution Plan Node.

This node orders a dataset, note that Heap Sort in this instance isn't the heap sort
algorithm, it is an approach where a heap of n items (the limit) is maintained as the
data passes through the operator. Because we are working with chunks, we build small
batches which we order and then discard the excess items.

This is faster, particularly when working with large datasets even though we're now
sorting smaller chunks over and over again.
"""

import time
from dataclasses import dataclass
from typing import Generator

import numpy
import pyarrow
import pyarrow.compute
from pyarrow import concat_tables

from opteryx.exceptions import ColumnNotFoundError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.operators.base_plan_node import BasePlanDataObject


@dataclass
class HeapSortDataObject(BasePlanDataObject):
    order_by: list = None
    limit: int = -1


class HeapSortNode(BasePlanNode):
    operator_type = OperatorType.BLOCKING

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.order_by = config.get("order_by", [])
        self.limit: int = config.get("limit", -1)

        self.do = HeapSortDataObject(order_by=self.order_by, limit=self.limit)

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return f"LIMIT = {self.limit} ORDER = " + ", ".join(
            f"{i[0].value} {i[1][0:3].upper()}" for i in self.order_by
        )

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    def execute(self) -> Generator[pyarrow.Table, None, None]:  # pragma: no cover
        table = None
        morsels = self._producers[0]  # type:ignore

        mapped_order = []

        for column, direction in self.order_by:
            try:
                mapped_order.append(
                    (
                        column.schema_column.identity,
                        direction,
                    )
                )
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                )

        for morsel in morsels.execute():
            start_time = time.time_ns()

            if table:
                # Concatenate the accumulated table with the new morsel
                table = concat_tables([table, morsel], promote_options="permissive")
            else:
                table = morsel

            # Determine if any columns are string-based
            use_pyarrow_sort = any(
                pyarrow.types.is_string(table.column(column_name).type)
                or pyarrow.types.is_binary(table.column(column_name).type)
                for column_name, _ in mapped_order
            )

            # strings are sorted faster user pyarrow, single columns faster using compute
            if len(mapped_order) == 1 and use_pyarrow_sort:
                column_name, sort_direction = mapped_order[0]
                column = table.column(column_name)
                if sort_direction == "ascending":
                    sort_indices = pyarrow.compute.sort_indices(column)
                else:
                    sort_indices = pyarrow.compute.sort_indices(column)[::-1]
                table = table.take(sort_indices[: self.limit])
            # strings are sorted faster using pyarrow
            elif use_pyarrow_sort:
                table = table.sort_by(mapped_order).slice(offset=0, length=self.limit)
            # single column sort using numpy
            elif len(mapped_order) == 1:
                # Single-column sort using mergesort to take advantage of partially sorted data
                column_name, sort_direction = mapped_order[0]
                column = table.column(column_name).to_numpy()
                if sort_direction == "ascending":
                    sort_indices = numpy.argsort(column)
                else:
                    sort_indices = numpy.argsort(column)[::-1]  # Reverse for descending
                # Slice the sorted table
                table = table.take(sort_indices[: self.limit])
            # multi column sort using numpy
            else:
                # Multi-column sort using lexsort
                columns_for_sorting = []
                directions = []
                for column_name, sort_direction in mapped_order:
                    column = table.column(column_name).to_numpy()
                    columns_for_sorting.append(column)
                    directions.append(1 if sort_direction == "ascending" else -1)

                sort_indices = numpy.lexsort(
                    [col[::direction] for col, direction in zip(columns_for_sorting, directions)]
                )
                # Slice the sorted table
                table = table.take(sort_indices[: self.limit])

            self.statistics.time_heap_sorting += time.time_ns() - start_time

        yield table
