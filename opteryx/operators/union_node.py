# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Union Node

This is a SQL Query Execution Plan Node.
"""

from pyarrow import Table

from opteryx import EOS
from opteryx.models import QueryProperties

from . import BasePlanNode


class UnionNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])
        self.column_ids = [c.schema_column.identity for c in self.columns]
        self.seen_first_eos = False
        self.schema = None

    @property
    def name(self):  # pragma: no cover
        return "Union"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel: Table, **kwargs) -> Table:
        """
        Union needs to ensure the column names are the same and that
        coercible types are coerced.
        """
        if morsel == EOS and self.seen_first_eos:
            yield EOS
            return
        elif morsel == EOS:
            self.seen_first_eos = True
            yield None
            return

        elif self.schema is None:
            self.schema = morsel.schema
        else:
            morsel = morsel.rename_columns(self.schema.names)
            morsel = morsel.cast(self.schema)

        yield morsel.select(self.column_ids)
