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
Sort Node

This is a SQL Query Execution Plan Node.

This node orders a dataset
"""
import time
from typing import Iterable
from typing import List

import numpy
from pyarrow import Table
from pyarrow import concat_tables

from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import SqlError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.models import Columns
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


class SortNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.order = config.get("order", [])

    @property
    def greedy(self):  # pragma: no cover
        return True

    @property
    def config(self):  # pragma: no cover
        return ", ".join([f"{i[0][0].value} {i[1][0:3].upper()}" for i in self.order])

    @property
    def name(self):  # pragma: no cover
        return "Sort"

    def execute(self) -> Iterable:
        if len(self._producers) != 1:  # pragma: no cover
            raise SqlError(f"{self.name} on expects a single producer")

        morsels = self._producers[0]  # type:ignore
        morsels = morsels.execute()
        morsels = tuple(morsels)
        mapped_order = []

        if len([morsel for morsel in morsels if morsel.num_rows == 0]) > 0:
            yield morsels[0]
            return

        table = concat_tables(morsels, promote=True)
        columns = Columns(table)

        start_time = time.time_ns()

        for column_list, direction in self.order:
            for column in column_list:
                if column.token_type == NodeType.FUNCTION:
                    # ORDER BY RAND() shuffles the results
                    # we create a random list, sort that then take the rows from the
                    # table in that order - this is faster than ordering the data
                    if column.value in ("RANDOM", "RAND"):
                        new_order = numpy.argsort(numpy.random.uniform(size=table.num_rows))
                        table = table.take(new_order)
                        self.statistics.time_ordering = time.time_ns() - start_time

                        yield table
                        return

                    raise SqlError("`ORDER BY` only supports `RAND()` as a functional sort order.")

                elif column.token_type == NodeType.LITERAL_NUMERIC:
                    # we have an index rather than a column name, it's a natural
                    # number but the list of column names is zero-based, so we
                    # subtract one
                    column_name = table.column_names[int(column.value) - 1]
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

        table = table.sort_by(mapped_order)
        self.statistics.time_ordering = time.time_ns() - start_time

        yield table
