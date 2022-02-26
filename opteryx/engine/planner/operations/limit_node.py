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
Limit Node

This is a SQL Query Execution Plan Node.

This Node returns up to a specified number of tuples.
"""
from typing import Iterable
from pyarrow import concat_tables
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class LimitNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._limit = config.get("limit")

    def execute(self, data_pages: Iterable) -> Iterable:

        result_set = []
        row_count = 0

        for page in data_pages:
            row_count += page.num_rows
            result_set.append(page)
            if row_count > self._limit:  # type:ignore
                break

        yield concat_tables(result_set).slice(0, self._limit)
