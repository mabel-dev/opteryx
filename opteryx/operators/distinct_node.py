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

import time
from typing import Generator

import pyarrow
import pyarrow.compute

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class DistinctNode(BasePlanNode):
    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        from opteryx.compiled.structures import HashSet

        super().__init__(properties=properties)
        self._distinct_on = config.get("on")
        if self._distinct_on:
            self._distinct_on = [col.schema_column.identity for col in self._distinct_on]
        self.hash_set = HashSet()

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    def execute(self) -> Generator[pyarrow.Table, None, None]:
        from opteryx.compiled.structures import distinct

        # We create a HashSet outside the distinct call, this allows us to pass
        # the hash to each run of the distinct which means we don't need to concat
        # all of the tables together to return a result.
        #
        # Being able to run morsel-by-morsel means if we have a LIMIT clause, we can
        # limit processing

        morsels = self._producers[0]  # type:ignore
        at_least_one = False

        for morsel in morsels.execute():
            start = time.monotonic_ns()
            unique_indexes, self.hash_set = distinct(
                morsel, columns=self._distinct_on, seen_hashes=self.hash_set
            )

            if len(unique_indexes) > 0:
                distinct_table = morsel.take(unique_indexes)
                self.statistics.time_distincting += time.monotonic_ns() - start
                yield distinct_table
                at_least_one = True
            elif not at_least_one:
                distinct_table = morsel.slice(0, 0)
                self.statistics.time_distincting += time.monotonic_ns() - start
                yield distinct_table
                at_least_one = True
