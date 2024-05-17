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
from typing import Generator

import numpy
from orso.types import OrsoTypes
from pyarrow import concat_tables

from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class SortNode(BasePlanNode):

    operator_type = OperatorType.BLOCKING

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.order_by = config.get("order", [])

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return ", ".join([f"{i[0].value} {i[1][0:3].upper()}" for i in self.order_by])

    @property
    def name(self):  # pragma: no cover
        return "Sort"

    def execute(self) -> Generator:
        morsels = self._producers[0]  # type:ignore
        morsels = morsels.execute()
        morsels = tuple(morsels)
        mapped_order = []

        if len([morsel for morsel in morsels if morsel.num_rows == 0]) > 0:
            yield morsels[0]
            return

        table = concat_tables(morsels, promote_options="permissive")

        start_time = time.time_ns()

        for column, direction in self.order_by:
            if column.node_type == NodeType.FUNCTION:
                # ORDER BY RAND() shuffles the results
                # we create a random list, sort that then take the rows from the
                # table in that order - this is faster than ordering the data
                if column.value in ("RANDOM", "RAND"):
                    new_order = numpy.argsort(numpy.random.uniform(size=table.num_rows))
                    table = table.take(new_order)
                    self.statistics.time_ordering = time.time_ns() - start_time

                    yield table
                    return

                raise UnsupportedSyntaxError(
                    "`ORDER BY` only supports `RAND()` as a functional sort order."
                )

            elif column.node_type == NodeType.LITERAL and column.type == OrsoTypes.INTEGER:
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
                            column.schema_column.identity,
                            direction,
                        )
                    )
                except ColumnNotFoundError as cnfe:  # pragma: no cover
                    raise ColumnNotFoundError(
                        f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                    )

        table = table.sort_by(mapped_order)
        self.statistics.time_ordering = time.time_ns() - start_time

        yield table
