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
    is_stateless = False

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")

        self.function_evaluations = get_all_nodes_of_type(
            self.filter,
            select_nodes=(NodeType.FUNCTION,),
        )

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Filter"

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        if morsel is EOS:
            yield EOS
            return

        if morsel.num_rows == 0:
            yield morsel
            return

        # Only evaluate expressions if necessary
        if self.function_evaluations:
            morsel = evaluate_and_append(self.function_evaluations, morsel)

        mask = evaluate(self.filter, morsel)

        # Ensure mask is a BooleanArray
        if not isinstance(mask, pyarrow.BooleanArray):
            mask = pyarrow.array(mask, type=pyarrow.bool_())

        indices = numpy.nonzero(mask)[0]

        if indices.size > 0:
            yield morsel.take(pyarrow.array(indices))
        else:
            yield morsel.slice(0, 0)
