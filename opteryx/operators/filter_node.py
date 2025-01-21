# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Selection Node

This is a SQL Query Execution Plan Node.

This node is responsible for applying filters to datasets.
"""

import multiprocessing

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

multiprocessing.set_start_method("fork", force=True)


def _parallel_filter(queue, morsel, function_evaluations, filters):
    if function_evaluations:
        morsel = evaluate_and_append(function_evaluations, morsel)
    mask = evaluate(filters, morsel)

    if not isinstance(mask, pyarrow.lib.BooleanArray):
        try:
            mask = pyarrow.array(mask, type=pyarrow.bool_())
        except Exception as err:  # nosec
            raise SqlError(f"Unable to filter on expression '{format_expression(filters)} {err}'.")

    mask = numpy.nonzero(mask)[0]
    # if there's no matching rows, don't return anything
    if mask.size > 0 and not numpy.all(mask is None):
        morsel = morsel.take(pyarrow.array(mask))
    else:
        morsel = morsel.slice(0, 0)

    if queue is not None:
        queue.put(morsel)
    else:
        return morsel


class FilterNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")

        self.function_evaluations = get_all_nodes_of_type(
            self.filter,
            select_nodes=(NodeType.FUNCTION,),
        )

        self.worker_count = pyarrow.io_thread_count() // 2

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

        if morsel.num_rows <= 10000 or self.worker_count <= 2:
            yield _parallel_filter(None, morsel, self.function_evaluations, self.filter)
        else:
            workers = []
            queue = multiprocessing.Queue()

            for block in morsel.to_batches((morsel.num_rows // self.worker_count) + 1):
                block = pyarrow.Table.from_batches([block])
                p = multiprocessing.Process(
                    target=_parallel_filter,
                    args=(queue, block, self.function_evaluations, self.filter),
                )
                p.start()
                workers.append(p)

            # Collect all results from the queue
            results = []
            for _ in workers:  # Expecting one result per worker
                results.append(queue.get())  # This will block until a result is available

            # Merge all results and return them
            if results:
                yield pyarrow.concat_tables(results)

            # Ensure all workers have finished before exiting
            for p in workers:
                p.join()
