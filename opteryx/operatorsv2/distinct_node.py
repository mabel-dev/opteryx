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

from pyarrow import Table

from opteryx import EOS
from opteryx.models import QueryProperties

from . import BasePlanNode


class DistinctNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        from opteryx.compiled.structures import HashSet

        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._distinct_on = parameters.get("on")
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

    def execute(self, morsel: Table) -> Table:
        from opteryx.compiled.structures import distinct

        # We create a HashSet outside the distinct call, this allows us to pass
        # the hash to each run of the distinct which means we don't need to concat
        # all of the tables together to return a result.
        #
        # Being able to run morsel-by-morsel means if we have a LIMIT clause, we can
        # limit processing

        if morsel == EOS:
            return EOS

        unique_indexes, self.hash_set = distinct(
            morsel, columns=self._distinct_on, seen_hashes=self.hash_set
        )

        if len(unique_indexes) > 0:
            distinct_table = morsel.take(unique_indexes)
            return distinct_table
        else:
            distinct_table = morsel.slice(0, 0)
            return distinct_table
