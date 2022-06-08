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
Database Reader Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from a database into a Table.
"""
import datetime

from typing import Iterable, Optional


import time

from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.storage.adapters import DiskStorage
from opteryx.storage.schemes import MabelPartitionScheme
from opteryx.utils.columns import Columns


class DatabaseReaderNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        """
        The Dataset Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(statistics=statistics, **config)

        from opteryx.engine.planner.planner import QueryPlanner

        today = datetime.datetime.utcnow().date()

        self._statistics = statistics
        self._alias, self._dataset = config.get("dataset", [None, None])

        if isinstance(self._dataset, (list, QueryPlanner, dict)):
            return

        self._dataset = self._dataset.replace(".", "/") + "/"
        self._reader = config.get("reader", DiskStorage())
        self._cache = config.get("cache")
        self._partition_scheme = config.get("partition_scheme", MabelPartitionScheme())

        self._start_date = config.get("start_date", today)
        self._end_date = config.get("end_date", today)

        # pushed down projection
        self._projection = config.get("projection")
        # pushed down selection
        self._selection = config.get("selection")

    @property
    def config(self):  # pragma: no cover
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        if isinstance(self._dataset, str):
            return self._dataset
        return "<complex dataset>"

    @property
    def name(self):  # pragma: no cover
        return "Database Dataset Reader"

    def execute(self, data_pages: Optional[Iterable]) -> Iterable:

        return []
