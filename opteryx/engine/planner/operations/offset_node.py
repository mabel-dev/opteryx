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
from pyarrow import concat_tables
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class OffsetNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._offset = config.get("offset")

    def execute(self, data_pages: Iterable) -> Iterable:

        row_count = 0

        for page in data_pages:
            if (row_count + page.num_rows) > self._offset:
                page = page.slice(
                    self._offset - row_count, page.num_rows  # type:ignore
                )
                yield page
                break
            else:
                row_count += page.num_rows

        yield from data_pages
