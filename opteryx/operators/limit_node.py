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

This Node performs the LIMIT and the OFFSET steps
"""
import time
from typing import Iterable

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.utils import arrow


class LimitNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.limit = config.get("limit")
        self.offset = config.get("offset", 0)

    @property
    def name(self):  # pragma: no cover
        return "LIMIT"

    @property
    def config(self):  # pragma: no cover
        return str(self.limit) + " OFFSET " + str(self.offset)

    def execute(self) -> Iterable:
        morsels = self._producers[0]  # type:ignore
        start_time = time.monotonic_ns()
        limited = arrow.limit_records(morsels.execute(), limit=self.limit, offset=self.offset)
        self.statistics.time_limiting += time.monotonic_ns() - start_time
        return limited
