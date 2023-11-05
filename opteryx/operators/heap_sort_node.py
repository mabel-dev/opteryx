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
data passes through the operator. Because we are working with chunks, we build a 
temporary batch of n + len(batch) items, sort them and then discard all but n of
the items.

This is faster, particularly when working with large datasets even though we're now
sorting smaller datasets over and over again.

Early testing with a ~10m record dataset:
- sort then limit: 39.2 seconds
- heapsort: 9.8 seconds
"""
import time
from typing import Iterable

import pyarrow
from orso.types import OrsoTypes
from pyarrow import Table

from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import SqlError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


class HeapSortNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.order = config.get("order", [])
        self.limit: int = config.get("limit", -1)

    @property
    def greedy(self):  # pragma: no cover
        return True

    @property
    def config(self):  # pragma: no cover
        return f"LIMIT = {self.limit} ORDER = " + ", ".join(
            [f"{i[0][0].value} {i[1][0:3].upper()}" for i in self.order]
        )

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    def execute(self) -> Iterable:
        morsels = self._producers[0]  # type:ignore
        if isinstance(morsels, Table):
            morsels = (morsels,)

        columns = None
        collected_rows = None
        mapped_order = []

        for morsel in morsels.execute():
            if columns is None:
                columns = Columns(morsel)

                for column_list, direction in self.order:
                    for column in column_list:
                        if (
                            column.node_type == NodeType.LITERAL
                            and column.type == OrsoTypes.INTEGER
                        ):
                            # we have an index rather than a column name, it's a natural
                            # number but the list of column names is zero-based, so we
                            # subtract one
                            column_name = morsel.column_names[int(column.value) - 1]
                            mapped_order.append(
                                (
                                    column_name,
                                    direction,
                                )
                            )
                        else:
                            try:
                                mapped_order.append(
                                    (
                                        columns.get_column_from_alias(
                                            format_expression(column), only_one=True
                                        ),
                                        direction,
                                    )
                                )
                            except ColumnNotFoundError as cnfe:
                                raise ColumnNotFoundError(
                                    f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                                )

            start_time = time.time_ns()

            # add what we've collected before to the table
            if collected_rows:  # pragma: no cover
                self.statistics.morsel_merges += 1
                morsel = pyarrow.concat_tables([collected_rows, morsel], mode="default")

            morsel = morsel.sort_by(mapped_order)
            collected_rows = morsel.slice(offset=0, length=self.limit)

            self.statistics.time_ordering += time.time_ns() - start_time

        yield collected_rows
