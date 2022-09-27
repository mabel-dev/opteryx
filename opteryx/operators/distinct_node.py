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

This Node eliminates duplicate records.
"""
from typing import Iterable

from pyarrow import Table, concat_tables

from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.third_party.pyarrow_ops import drop_duplicates


class DistinctNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._distinct = config.get("distinct", True)

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def greedy(self):  # pragma: no cover
        return True

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, Table):
            data_pages = (data_pages,)

        if self._distinct:
            yield drop_duplicates(concat_tables(data_pages.execute(), promote=True))
            return
        yield from data_pages
