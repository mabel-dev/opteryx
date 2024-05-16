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
import time
from dataclasses import dataclass
from typing import Generator
from typing import Set
from typing import Tuple

import numpy
import pyarrow
from orso.schema import FlatColumn

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.operators.base_plan_node import BasePlanDataObject

INTERNAL_BATCH_SIZE: int = 2500  # config
MAX_JOIN_SIZE: int = 1000  # config
MORSEL_SIZE_BYTES: int = 32 * 1024 * 1024


def _cross_join_unnest_column(
    morsels: BasePlanNode = None,
    source: Node = None,
    target_column: FlatColumn = None,
    filters: Set = None,
    statistics=None,
) -> Generator[pyarrow.Table, None, None]:
    """
    Perform a cross join on an unnested column of pyarrow tables.

    Args:
        morsels: An iterable of `pyarrow.Table` objects to be cross joined.
        source: The source node indicating the column.
        target_column: The column to be unnested.

    Returns:
        A generator that yields the resulting `pyarrow.Table` objects.
    """
    from opteryx.compiled.cross_join import build_filtered_rows_indices_and_column
    from opteryx.compiled.cross_join import build_rows_indices_and_column

    # Check if the source node type is an identifier, raise error otherwise
    if source.node_type != NodeType.IDENTIFIER:
        raise NotImplementedError("Can only CROSS JOIN UNNEST on a column")

    column_type = None
    batch_size: int = INTERNAL_BATCH_SIZE
    return_morsel: pyarrow.Table = None
    at_least_once = False

    # Loop through each morsel from the morsels execution
    for left_morsel in morsels.execute():
        start = time.monotonic_ns()
        # Break the morsel into batches to avoid memory issues
        for left_block in left_morsel.to_batches(max_chunksize=batch_size):
            # Fetch the data of the column to be unnested
            column_data = left_block[source.schema_column.identity]
            # we need the offsets before we drop the rows
            valid_offsets = column_data.is_valid()
            column_data = column_data.drop_null()
            # if there's no valid records, continue to the next record
            if len(column_data) == 0:
                continue
            # drop the rows here, wait until we know we need to
            left_block = left_block.filter(valid_offsets)
            # Set column_type if it hasn't been determined already
            if column_type is None:
                column_type = column_data.type.value_type

            if filters is None:
                indices, new_column_data = build_rows_indices_and_column(
                    column_data.to_numpy(False)
                )
            else:
                indices, new_column_data = build_filtered_rows_indices_and_column(
                    column_data.to_numpy(False), filters
                )

            new_block = left_block.take(indices)
            new_block = pyarrow.Table.from_batches([new_block], schema=left_morsel.schema)
            new_block = new_block.append_column(target_column.identity, [new_column_data])

            statistics.time_cross_join_unnest += time.monotonic_ns() - start

            if return_morsel is None or return_morsel.num_rows == 0:
                return_morsel = new_block
            elif new_block.num_rows > 0:
                return_morsel = pyarrow.concat_tables(
                    [return_morsel, new_block], promote_options="none"
                )
                if return_morsel.nbytes > MORSEL_SIZE_BYTES:
                    yield return_morsel
                    at_least_once = True
                    return_morsel = None
            start = time.monotonic_ns()

    if return_morsel.num_rows > 0:
        at_least_once = True
        yield return_morsel

    if not at_least_once:
        yield new_block.slice()


def _cross_join_unnest_literal(
    morsels: BasePlanNode, source: Tuple, target_column: FlatColumn, statistics
) -> Generator[pyarrow.Table, None, None]:
    joined_list_size = len(source)

    # Loop through each morsel from the morsels execution
    for left_morsel in morsels.execute():
        start = time.monotonic_ns()
        # Break the morsel into batches to avoid memory issues
        for left_block in left_morsel.to_batches(max_chunksize=INTERNAL_BATCH_SIZE):
            left_block = pyarrow.Table.from_batches([left_block], schema=left_morsel.schema)
            block_size = left_block.num_rows

            # Repeat each row in the table n times
            repeated_indices = numpy.repeat(numpy.arange(block_size), joined_list_size)
            appended_table = left_block.take(repeated_indices)

            # Tile the array to match the new number of rows
            tiled_array = numpy.tile(source, block_size)

            # Convert tiled_array to PyArrow array and append it to the table
            array_column = pyarrow.array(tiled_array)
            appended_table = appended_table.append_column(target_column.identity, array_column)

            statistics.time_cross_join_unnest += time.monotonic_ns() - start
            yield appended_table
            start = time.monotonic_ns()


def _cartesian_product(*arrays):
    """
    Cartesian product of arrays creates every combination of the elements in the arrays
    """
    array_count = len(arrays)
    arr = numpy.empty([len(array) for array in arrays] + [array_count], dtype=numpy.int64)
    for i, array in enumerate(numpy.ix_(*arrays)):
        arr[..., i] = array
    return numpy.hsplit(arr.reshape(-1, array_count), array_count)


def _cross_join(left, right, statistics):
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

    from opteryx.utils.arrow import align_tables

    for left_morsel in left.execute():
        start = time.monotonic_ns()
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
                statistics.time_cross_join_unnest += time.monotonic_ns() - start
                yield table
                start = time.monotonic_ns()


@dataclass
class CrossJoinDataObject(BasePlanDataObject):
    source: str = None
    _unnest_column: str = None
    _unnest_target: str = None
    _filters: str = None


class CrossJoinNode(BasePlanNode):
    """
    Implements a SQL CROSS JOIN
    """

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

        self.source = config.get("column")

        # do we have unnest details?
        self._unnest_column = config.get("unnest_column")
        self._unnest_target = config.get("unnest_target")
        self._filters = config.get("filters")

        # handle variation in how the unnested column is represented
        if self._unnest_column:
            if self._unnest_column.node_type == NodeType.NESTED:
                self._unnest_column = self._unnest_column.centre
            # if we have a literal that's not a tuple, wrap it
            if self._unnest_column.node_type == NodeType.LITERAL and not isinstance(
                self._unnest_column.value, tuple
            ):
                self._unnest_column.value = tuple([self._unnest_column.value])

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore
        right_table = pyarrow.concat_tables(
            right_node.execute(), promote_options="none"
        )  # type:ignore

        if self._unnest_column is None:
            yield from _cross_join(left_node, right_table, self.statistics)

        elif isinstance(self._unnest_column.value, tuple):
            yield from _cross_join_unnest_literal(
                morsels=left_node,
                source=self._unnest_column.value,
                target_column=self._unnest_target,
                statistics=self.statistics,
            )
        else:
            yield from _cross_join_unnest_column(
                morsels=left_node,
                source=self._unnest_column,
                target_column=self._unnest_target,
                filters=self._filters,
                statistics=self.statistics,
            )
