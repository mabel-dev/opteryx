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
Cross Join Node

This is a SQL Query Execution Plan Node.

This performs a CROSS JOIN - CROSS JOIN is not natively supported by PyArrow so this is written
here rather than calling the join() functions
"""
import typing

import numpy
import pyarrow

from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode

INTERNAL_BATCH_SIZE = 100  # config
MAX_JOIN_SIZE = 500  # config


def _cartesian_product(*arrays):
    """
    Cartesian product of arrays creates every combination of the elements in the arrays
    """
    array_count = len(arrays)
    arr = numpy.empty([len(array) for array in arrays] + [array_count], dtype=numpy.int64)
    for i, array in enumerate(numpy.ix_(*arrays)):
        arr[..., i] = array
    return numpy.hsplit(arr.reshape(-1, array_count), array_count)


def _cross_join(left, right):
    """
    A cross join is the cartesian product of two tables - this usually isn't very
    useful, but it does allow you to the theta joins (non-equi joins)
    """

    def _chunker(seq_1, seq_2, size):
        """
        Chunk two equal length interables into size sized chunks

        This returns a generator.
        """
        return (
            (seq_1[pos : pos + size], seq_2[pos : pos + size]) for pos in range(0, len(seq_1), size)
        )

    from opteryx.third_party.pyarrow_ops import align_tables

    for left_morsel in left.execute():
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
            for left_chunk, right_chunk in _chunker(left_align, right_align, MAX_JOIN_SIZE):
                # Align the tables using the specified chunks of row indices
                table = align_tables(left_block, right, left_chunk.flatten(), right_chunk.flatten())

                # Yield the resulting table to the caller
                yield table


class CrossJoinNode(BasePlanNode):
    """
    Implements a SQL CROSS JOIN
    """

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.join_type = config["type"]

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> typing.Iterable:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore
        right_table = pyarrow.concat_tables(right_node.execute(), promote=True)  # type:ignore

        yield from _cross_join(left_node, right_table)
