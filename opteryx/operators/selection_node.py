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

from opteryx.attribute_types import TOKEN_TYPES
from opteryx.exceptions import SqlError
from opteryx.managers.expression import evaluate
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.utils.arrow import consolidate_pages


class SelectionNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.filter = config.get("filter")
        self._unfurled_filter = None
        self._mapped_filter = None

    @property
    def config(self):  # pragma: no cover
        def _inner_config(predicate):
            if isinstance(predicate, tuple):
                if len(predicate) > 1 and predicate[1] == TOKEN_TYPES.IDENTIFIER:
                    return f"`{predicate[0]}`"
                if len(predicate) > 1 and predicate[1] == TOKEN_TYPES.VARCHAR:
                    return f'"{predicate[0]}"'
                if len(predicate) == 2:
                    if predicate[0] == "Not":
                        return f"NOT {_inner_config(predicate[1])}"
                    return f"{predicate[0]}"
                return "(" + " ".join(_inner_config(p) for p in predicate) + ")"
            if isinstance(predicate, list):
                if len(predicate) == 1:
                    return _inner_config(predicate[0])
                return "[" + ",".join(_inner_config(p) for p in predicate) + "]"
            return f"{predicate}"

        return _inner_config(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Selection"

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, Table):
            data_pages = (data_pages,)

        # we should always have a filter - but no harm in checking
        if self.filter is None:
            yield from data_pages

        else:

            for page in consolidate_pages(
                data_pages.execute(),
                self._statistics,
                self.properties.enable_page_management,
            ):

                start_selection = time.time_ns()
                mask = evaluate(self.filter, page)
                self._statistics.time_evaluating += time.time_ns() - start_selection

                # if the mask is a boolean array, we've called a function that
                # returns booleans
                if isinstance(mask, pyarrow.lib.BooleanArray) or (
                    isinstance(mask, numpy.ndarray) and mask.dtype == numpy.bool_
                ):
                    mask = numpy.nonzero(mask)[0]

                self._statistics.time_selecting += time.time_ns() - start_selection
                yield page.take(mask)
