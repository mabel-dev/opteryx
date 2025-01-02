# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Limit Node

This is a SQL Query Execution Plan Node.

This Node performs the LIMIT and the OFFSET steps
"""

import pyarrow

from opteryx import EOS
from opteryx.models import QueryProperties

from . import BasePlanNode


class LimitNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.limit = parameters.get("limit", float("inf"))
        self.offset = parameters.get("offset", 0)

        self.remaining_rows = self.limit if self.limit is not None else float("inf")
        self.rows_left_to_skip = max(0, self.offset)

    @property
    def name(self):  # pragma: no cover
        return "LIMIT"

    @property
    def config(self):  # pragma: no cover
        return str(self.limit) + " OFFSET " + str(self.offset)

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        if morsel == EOS:
            yield EOS
            return

        if self.rows_left_to_skip > 0:
            if self.rows_left_to_skip >= morsel.num_rows:
                self.rows_left_to_skip -= morsel.num_rows
                yield morsel.slice(offset=0, length=0)
                return
            else:
                morsel = morsel.slice(
                    offset=self.rows_left_to_skip, length=morsel.num_rows - self.rows_left_to_skip
                )
                self.rows_left_to_skip = 0

        if self.remaining_rows <= 0 or morsel.num_rows == 0:
            yield morsel.slice(offset=0, length=0)

        elif morsel.num_rows < self.remaining_rows:
            self.remaining_rows -= morsel.num_rows
            yield morsel
        else:
            rows_to_slice = self.remaining_rows
            self.remaining_rows = 0
            yield morsel.slice(offset=0, length=rows_to_slice)
