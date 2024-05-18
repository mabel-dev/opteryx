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

import pyarrow
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

            if morsel.num_rows > self.limit:
                # not much point doing this here if we're not eliminating rows
                morsel = morsel.sort_by(mapped_order)
                morsel = morsel.slice(offset=0, length=self.limit)

            if table:
                # Concatenate the current morsel with the previously accumulated table
                morsel = concat_tables([morsel, table], promote_options="permissive")

            # Sort and slice the concatenated table to maintain the limit
            morsel = morsel.sort_by(mapped_order)
            morsel = morsel.slice(offset=0, length=self.limit)
            table = morsel

            self.statistics.time_heap_sorting += time.time_ns() - start_time

        yield table
