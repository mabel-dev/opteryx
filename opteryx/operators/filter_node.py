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
    is_stateless = True

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
        morsel = self.ensure_arrow_table(morsel)

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

        # Convert mask to PyArrow BooleanArray if needed, then use PyArrow's filter
        # This is significantly faster than converting to indices and using take()
        # Benchmark shows ~8-9x speedup over numpy mask + take approach
        if isinstance(mask, pyarrow.BooleanArray):
            # Already PyArrow - use directly
            filtered = morsel.filter(mask)
        elif isinstance(mask, numpy.ndarray):
            # Convert numpy boolean to PyArrow BooleanArray
            # PyArrow's filter handles null values correctly in Kleene logic
            filtered = morsel.filter(pyarrow.array(mask, type=pyarrow.bool_()))
        elif isinstance(mask, list):
            # Convert list to PyArrow BooleanArray
            filtered = morsel.filter(pyarrow.array(mask, type=pyarrow.bool_()))
        else:
            # Generic fallback: convert to numpy boolean, then to PyArrow
            mask_np = numpy.asarray(mask, dtype=numpy.bool_)
            filtered = morsel.filter(pyarrow.array(mask_np, type=pyarrow.bool_()))

        if filtered.num_rows > 0:
            yield filtered
        else:
            yield morsel.slice(0, 0)
