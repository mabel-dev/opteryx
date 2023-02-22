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
Selection Node

This is a SQL Query Execution Plan Node.
"""
import time
from typing import Iterable

import numpy
import pyarrow
from pyarrow import Table

from opteryx.exceptions import SqlError
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import format_expression
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


class SelectionNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.filter = config.get("filter")

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Selection"

    def execute(self) -> Iterable:
        if len(self._producers) != 1:  # pragma: no cover
            raise SqlError(f"{self.name} on expects a single producer")

        morsels = self._producers[0]  # type:ignore
        schema = None
        at_least_one = False

        # we should always have a filter - but no harm in checking
        if self.filter is None:
            yield from morsels.execute()
            return

        for morsel in morsels.execute():
            if schema is None:
                schema = morsel.schema

            if morsel.num_rows == 0:
                continue

            start_selection = time.time_ns()
            mask = evaluate(self.filter, morsel, False)
            self.statistics.time_evaluating += time.time_ns() - start_selection

            # if the mask is a boolean array, we've called a function that
            # returns booleans
            if isinstance(mask, pyarrow.lib.BooleanArray) or (
                isinstance(mask, numpy.ndarray) and mask.dtype == numpy.bool_
            ):
                mask = numpy.nonzero(mask)[0]

            self.statistics.time_selecting += time.time_ns() - start_selection

            # if there's no matching rows, just drop the morsel
            if mask.size > 0:
                yield morsel.take(pyarrow.array(mask))
                at_least_one = True

        # we need to send something to the next operator, send an empty table
        if not at_least_one:
            yield pyarrow.Table.from_arrays([[] for i in schema.names], schema=schema)
