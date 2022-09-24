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
Offset Node

This is a SQL Query Execution Plan Node.

This Node skips over tuples.
"""
from typing import Iterable

import pyarrow

from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


class OffsetNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._offset = config.get("offset")

    @property
    def name(self):  # pragma: no cover
        return "Offset"

    @property
    def config(self):  # pragma: no cover
        return str(self._offset)

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, pyarrow.Table):
            data_pages = (data_pages,)

        row_count = 0

        iterator = data_pages.execute()

        for page in iterator:
            if (row_count + page.num_rows) > self._offset:
                page = page.slice(
                    self._offset - row_count, page.num_rows  # type:ignore
                )
                yield page
                break
            row_count += page.num_rows

        yield from iterator
