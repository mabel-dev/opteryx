# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Cross Join Node

This is a SQL Query Execution Plan Node.

This performs a CROSS JOIN - CROSS JOIN is not natively supported by PyArrow so this is written
here rather than calling the join() functions

Note: CROSS JOIN UNNEST is implemented by the UnnestJoinNode
"""

import numpy
import pyarrow

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.third_party.abseil.containers import FlatHashSet

from . import JoinNode

INTERNAL_BATCH_SIZE: int = 10000  # config
MAX_JOIN_SIZE: int = 1_000_000  # config


def _cartesian_product(*arrays):
    """
    Cartesian product of arrays creates every combination of the elements in the arrays
    """
    array_count = len(arrays)
    arr = numpy.empty([len(array) for array in arrays] + [array_count], dtype=numpy.int64)
    for i, array in enumerate(numpy.ix_(*arrays)):
        arr[..., i] = array
    return numpy.hsplit(arr.reshape(-1, array_count), array_count)


def _cross_join(left_morsel, right):
    """
    A cross join is the cartesian product of two tables - this usually isn't very
    useful, but it does allow you to the theta joins (non-equi joins)
    """

    def _chunker(seq_1, seq_2, size: int = INTERNAL_BATCH_SIZE):
        for i in range(0, len(seq_1), size):
            yield memoryview(seq_1)[i : i + size], memoryview(seq_2)[i : i + size]

    from opteryx.utils.arrow import align_tables

    at_least_once = False
    left_schema = left_morsel.schema
    right_schema = right.schema

    # Iterate through left table in chunks of size INTERNAL_BATCH_SIZE
    for left_block in left_morsel.to_batches(max_chunksize=INTERNAL_BATCH_SIZE):
        # Convert the chunk to a table to retain column names
        left_block = pyarrow.Table.from_batches([left_block], schema=left_morsel.schema)

        # Create an array of row indices for each table
        left_array = numpy.arange(left_block.num_rows, dtype=numpy.int64)
        right_array = numpy.arange(right.num_rows, dtype=numpy.int64)

        # Calculate the cartesian product of the two arrays of row indices
        left_align, right_align = _cartesian_product(left_array, right_array)

        # Further break down the result into manageable chunks of size MAX_JOIN_SIZE
        for left_chunk, right_chunk in _chunker(
            left_align.flatten(), right_align.flatten(), MAX_JOIN_SIZE
        ):
            # Align the tables using the specified chunks of row indices
            table = align_tables(left_block, right, left_chunk, right_chunk)

            # Yield the resulting table to the caller
            yield table
            at_least_once = True

    if not at_least_once:
        fields = [pyarrow.field(name=f.name, type=f.type) for f in right_schema] + [
            pyarrow.field(name=f.name, type=f.type) for f in left_schema
        ]
        combined_schemas = pyarrow.schema(fields)
        yield pyarrow.Table.from_arrays(
            [pyarrow.array([]) for _ in combined_schemas], schema=combined_schemas
        )


class CrossJoinNode(JoinNode):
    """
    Implements a SQL CROSS JOIN
    """

    join_type = "cross"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.source = parameters.get("column")

        self._left_relation = parameters.get("left_relation_names")
        self._right_relation = parameters.get("right_relation_names")

        self.left_buffer = []
        self.right_buffer = []
        self.left_relation = None
        self.right_relation = None
        self.hash_set = FlatHashSet()

        self.continue_executing = True

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        return f"CROSS JOIN"

    def execute(self, morsel: pyarrow.Table, join_leg: str) -> pyarrow.Table:
        if not self.continue_executing:
            yield None
            return

        if join_leg == "left":
            if morsel == EOS:
                self.left_relation = pyarrow.concat_tables(self.left_buffer, promote_options="none")
                self.left_buffer.clear()
            else:
                self.left_buffer.append(morsel)
            yield None
            return

        if join_leg == "right":
            if morsel == EOS:
                right_table = pyarrow.concat_tables(self.right_buffer, promote_options="none")  # type:ignore
                self.right_buffer = None
                yield from _cross_join(self.left_relation, right_table)
                yield EOS
            else:
                self.right_buffer.append(morsel)
                yield None
