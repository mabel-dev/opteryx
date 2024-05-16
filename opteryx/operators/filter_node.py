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

This node is responsible for applying filters to datasets.
"""
import time
from typing import Generator

import numpy
import pyarrow

from opteryx.exceptions import SqlError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class FilterNode(BasePlanNode):

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.filter = config.get("filter")

        self.function_evaluations = get_all_nodes_of_type(
            self.filter,
            select_nodes=(NodeType.FUNCTION,),
        )

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Filter"

    def execute(self) -> Generator:
        morsels = self._producers[0]  # type:ignore
        schema = None
        at_least_one = False

        for morsel in morsels.execute():
            if schema is None:
                schema = morsel.schema

            if morsel.num_rows == 0:
                continue

            start_selection = time.time_ns()
            morsel = evaluate_and_append(self.function_evaluations, morsel)
            mask = evaluate(self.filter, morsel)
            self.statistics.time_evaluating += time.time_ns() - start_selection

            if not isinstance(mask, pyarrow.lib.BooleanArray):
                try:
                    mask = pyarrow.array(mask, type=pyarrow.bool_())
                except Exception as err:
                    raise SqlError(
                        f"Unable to filter on expression '{format_expression(self.filter)}'."
                    )
            mask = numpy.nonzero(mask)[0]

            self.statistics.time_selecting += time.time_ns() - start_selection

            # if there's no matching rows, just drop the morsel
            if mask.size > 0 and not numpy.all(mask is None):
                yield morsel.take(pyarrow.array(mask))
                at_least_one = True

        # we need to send something to the next operator, send an empty table
        if not at_least_one:
            yield pyarrow.Table.from_arrays([[] for i in schema.names], schema=schema)
