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
from opteryx.draken import Morsel
from opteryx.models import QueryProperties

from . import BasePlanNode


class DistinctNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._distinct_on = parameters.get("on")
        if self._distinct_on:
            # Convert column identities to bytes for Draken morsel
            self._distinct_on = [
                col.schema_column.identity.encode("utf-8") for col in self._distinct_on
            ]
        self.hash_set = None
        self.at_least_one_yielded = False

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    def execute(self, morsel: Table, **kwargs) -> Table:
        from opteryx.compiled.table_ops.distinct import distinct

        if morsel == EOS:
            yield EOS
            return

        morsel = [morsel] if isinstance(morsel, Morsel) else Morsel.iter_from_arrow(morsel)

        for chunk in morsel:
            # Use Draken-based distinct with column names as bytes
            unique_indexes, self.hash_set = distinct(
                chunk, columns=self._distinct_on, seen_hashes=self.hash_set
            )

            if len(unique_indexes) > 0:
                chunk.take(unique_indexes)
                yield chunk
            elif not self.at_least_one_yielded:
                chunk.empty()
                yield chunk

            self.at_least_one_yielded = True
