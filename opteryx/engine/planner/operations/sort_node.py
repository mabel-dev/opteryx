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
from typing import Iterable, List

from pyarrow import Table, concat_tables

from opteryx.engine.functions import FUNCTIONS
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.exceptions import SqlError
from opteryx.utils.columns import Columns


class SortNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
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

        if len([page for page in data_pages if page.num_rows == 0]):
            yield data_pages[0]
            return

        table = concat_tables(data_pages)
        columns = Columns(table)
        need_to_remove_random = False

        for column, direction in self._order:

            # function references aere recorded as dictionaries
            if isinstance(column, dict):

                # we only have special handling for RANDOM at the moment
                if column["alias"] != "RANDOM()":
                    if len(columns.get_column_from_alias(column["alias"])) == 0:
                        raise SqlError(
                            "ORDER BY can only reference functions used in the SELECT clause, or RANDOM()"
                        )

                    self._mapped_order.append(
                        (
                            columns.get_column_from_alias(
                                column["alias"], only_one=True
                            ),
                            direction,
                        )
                    )
                else:
                    # this currently only supports zero parameter functions
                    calculated_values = FUNCTIONS[column["function"]](*[table.num_rows])

                    table = Table.append_column(
                        table, column["alias"], calculated_values
                    )
                    # we add it to sort, but it's not in the SELECT so we shouldn't return it
                    need_to_remove_random = True

                    self._mapped_order.append(
                        (
                            column["alias"],
                            direction,
                        )
                    )

            else:
                self._mapped_order.append(
                    (
                        columns.get_column_from_alias(column, only_one=True),
                        direction,
                    )
                )

        table = table.sort_by(self._mapped_order)

        if need_to_remove_random:
            table = table.drop(["RANDOM()"])

        yield table
