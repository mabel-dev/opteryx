# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

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
        from opteryx.third_party.abseil.containers import FlatHashSet as HashSet

        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._distinct_on = parameters.get("on")
        if self._distinct_on:
            self._distinct_on = [col.schema_column.identity for col in self._distinct_on]
        self.hash_set = HashSet()

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    def execute(self, morsel: Table, **kwargs) -> Table:
        from opteryx.compiled.table_ops.distinct import distinct

        # We create a HashSet outside the distinct call, this allows us to pass
        # the hash to each run of the distinct which means we don't need to concat
        # all of the tables together to return a result.
        #
        # Being able to run morsel-by-morsel means if we have a LIMIT clause, we can
        # limit processing

        if morsel == EOS:
            yield EOS
            return

        unique_indexes, self.hash_set = distinct(
            morsel, columns=self._distinct_on, seen_hashes=self.hash_set
        )

        if len(unique_indexes) > 0:
            distinct_table = morsel.take(unique_indexes)
            yield distinct_table
        else:
            distinct_table = morsel.slice(0, 0)
            yield distinct_table
