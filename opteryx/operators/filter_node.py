# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Selection Node

This is a SQL Query Execution Plan Node.

This node is responsible for applying filters to datasets.
"""

import numpy
import pyarrow

from opteryx import EOS
from opteryx.exceptions import SqlError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import format_expression
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties

from . import BasePlanNode


class FilterNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")

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

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        if morsel == EOS:
            yield EOS
            return

        if morsel.num_rows == 0:
            yield morsel
            return

        if self.function_evaluations:
            morsel = evaluate_and_append(self.function_evaluations, morsel)
        mask = evaluate(self.filter, morsel)

        if not isinstance(mask, pyarrow.lib.BooleanArray):
            try:
                mask = pyarrow.array(mask, type=pyarrow.bool_())
            except Exception as err:  # nosec
                raise SqlError(
                    f"Unable to filter on expression '{format_expression(self.filter)} {err}'."
                )
        mask = numpy.nonzero(mask)[0]

        # if there's no matching rows, return empty morsel
        if mask.size > 0 and not numpy.all(mask is None):
            yield morsel.take(pyarrow.array(mask))
        else:
            yield morsel.slice(0, 0)
