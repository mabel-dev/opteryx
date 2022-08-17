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

from typing import Iterable, List

from pyarrow import Table, concat_tables

from opteryx.engine import QueryDirectives, QueryStatistics
from opteryx.engine.planner.expression import evaluate_and_append
from opteryx.engine.planner.expression import ExpressionTreeNode
from opteryx.engine.planner.expression import format_expression
from opteryx.engine.planner.expression import NodeType
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.exceptions import SqlError
from opteryx.utils.columns import Columns


class SortNode(BasePlanNode):
    def __init__(
        self, directives: QueryDirectives, statistics: QueryStatistics, **config
    ):
        super().__init__(directives=directives, statistics=statistics)
        self._order = config.get("order", [])
        self._mapped_order: List = []

    @property
    def greedy(self):  # pragma: no cover
        return True

    @property
    def config(self):  # pragma: no cover
        return ",".join([str(i) for i in self._order])

    @property
    def name(self):  # pragma: no cover
        return "Sort"

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, Table):
            data_pages = (data_pages,)

        data_pages = data_pages.execute()
        data_pages = tuple(data_pages)

        if len([page for page in data_pages if page.num_rows == 0]) > 0:
            yield data_pages[0]
            return

        table = concat_tables(data_pages, promote=True)
        original_columns = table.column_names
        columns = Columns(table)

        start_time = time.time_ns()

        for column_list, direction in self._order:

            for column in column_list:
                if column.token_type == NodeType.FUNCTION:
                    columns, expressions, table = evaluate_and_append([column], table)
                    self._mapped_order.append(
                        (
                            columns.get_column_from_alias(
                                format_expression(column), only_one=True
                            ),
                            direction,
                        )
                    )
                elif column.token_type == NodeType.LITERAL_NUMERIC:

                    # we have an index rather than a column name, it's a natural
                    # number but the list of column names is zero-based, so we
                    # subtract one
                    column_name = table.column_names[int(column.value) - 1]
                    self._mapped_order.append(
                        (
                            column_name,
                            direction,
                        )
                    )
                else:
                    self._mapped_order.append(
                        (
                            columns.get_column_from_alias(column.value, only_one=True),
                            direction,
                        )
                    )

        table = table.sort_by(self._mapped_order)

        # remove any columns we added just for ordering
        table = table.select(original_columns)

        self._statistics.time_ordering = time.time_ns() - start_time

        yield table
