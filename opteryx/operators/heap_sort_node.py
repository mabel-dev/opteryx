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
data passes through the operator. Because we are working with pages, we build a 
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

from pyarrow import Table

from opteryx.exceptions import ColumnNotFoundError, SqlError
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import NodeType
from opteryx.models import Columns, QueryProperties
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
        return ",".join([str(i) for i in self.order])

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, Table):
            data_pages = (data_pages,)

        columns = None
        collected_rows = None
        mapped_order = []

        for page in data_pages.execute():

            if columns is None:
                columns = Columns(page)

                for column_list, direction in self.order:
                    for column in column_list:
                        if column.token_type == NodeType.LITERAL_NUMERIC:
                            # we have an index rather than a column name, it's a natural
                            # number but the list of column names is zero-based, so we
                            # subtract one
                            column_name = page.column_names[int(column.value) - 1]
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
                self.statistics.page_merges += 1
                page = pyarrow.concat_tables([collected_rows, page], promote=True)

            page = page.sort_by(mapped_order)
            collected_rows = page.slice(offset=0, length=self.limit)

            self.statistics.time_ordering += time.time_ns() - start_time

        yield collected_rows
