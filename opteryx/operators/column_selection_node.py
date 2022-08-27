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
Distinct Node

This is a SQL Query Execution Plan Node.

This Node is used to evaluate the filters on SHOW COLUMNS
"""
from typing import Iterable

from opteryx.models import Columns, QueryDirectives, QueryStatistics
from opteryx.operators import BasePlanNode
from opteryx.exceptions import SqlError


class ColumnSelectionNode(BasePlanNode):
    def __init__(
        self, directives: QueryDirectives, statistics: QueryStatistics, **config
    ):
        super().__init__(directives=directives, statistics=statistics)
        self._filter = config.get("filter", True)

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def greedy(self):  # pragma: no cover
        return True

    @property
    def name(self):  # pragma: no cover
        return "Select Columns"

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore

        selection = None
        columns = None

        for page in data_pages.execute():
            if selection is None:
                columns = Columns(page)
                selection = list(set(columns.filter(self._filter)))
            page = page.select(selection)
            yield columns.apply(page)
