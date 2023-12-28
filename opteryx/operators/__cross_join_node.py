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
from orso.schema import FlatColumn

from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedTypeError
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode

INTERNAL_BATCH_SIZE = 100  # config
MAX_JOIN_SIZE = 500  # config


def _cross_join_unnest_column(
    morsels: typing.Iterable[pyarrow.Table], source: Node, target_column: FlatColumn
) -> typing.Generator[pyarrow.Table, None, None]:
    """
    Perform a cross join on an unnested column of pyarrow tables.

    Args:
        morsels: An iterable of `pyarrow.Table` objects to be cross joined.
        source: The source node indicating the column.
        target_column: The column to be unnested.

    Returns:
        A generator that yields the resulting `pyarrow.Table` objects.
    """

    # Check if the source node type is an identifier, raise error otherwise
    if source.node_type != NodeType.IDENTIFIER:
        raise NotImplementedError("Can only CROSS JOIN UNNEST on a column")

    column_type = None

    # Loop through each morsel from the morsels execution
    for left_morsel in morsels.execute():
        # Break the morsel into batches to avoid memory issues
        for left_block in left_morsel.to_batches(max_chunksize=INTERNAL_BATCH_SIZE):
            # Fetch the data of the column to be unnested
            column_data = left_block[source.schema_column.identity]

            # Set column_type if it hasn't been determined already
            if column_type is None:
                column_type = column_data.type.value_type

            # Initialize a dictionary to store new column data
            columns_data = {name: [] for name in left_block.schema.names}
            new_column_data = []

            # Loop through each value in the column data
            for i, value in enumerate(column_data):
                # Check if value is valid and non-empty
                if value.is_valid and len(value) != 0:
                    # Determine how many times a row needs to be repeated based on the length of the unnest value
                    repeat_count = len(value)

                    # Repeat the data for each column accordingly
                    for col_name in left_block.schema.names:
                        columns_data[col_name].extend(
                            [left_block.column(col_name)[i].as_py()] * repeat_count
                        )

                    # Extend the new column data with the unnested values
                    new_column_data.extend(value.as_py())
                else:
                    # If value is not valid or empty, just append the existing data
                    for col_name in left_block.schema.names:
                        columns_data[col_name].append(left_block.column(col_name)[i].as_py())
                    new_column_data.append(None)

            # If no new data was generated, skip to next iteration
            if not columns_data:
                continue

            # Convert lists to pyarrow arrays for each column
            for col_name, col_data in columns_data.items():
                columns_data[col_name] = pyarrow.array(col_data)

            # Convert new column data to pyarrow array and set its type
            columns_data[target_column.identity] = pyarrow.array(new_column_data, type=column_type)

            # Create a new table from the arrays and yield the result
            new_block = pyarrow.Table.from_arrays(
                list(columns_data.values()), names=list(columns_data.keys())
            )
            yield new_block


def _cross_join_unnest_literal(
    morsels: typing.Iterable[pyarrow.Table], source: Node, target_column: FlatColumn
) -> typing.Generator[pyarrow.Table, None, None]:
    joined_list_size = len(source)

    # Loop through each morsel from the morsels execution
    for left_morsel in morsels.execute():
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

            yield appended_table


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

        self.source = config.get("column")

        # do we have unnest details?
        self._unnest_column = config.get("unnest_column")
        self._unnest_target = config.get("unnest_target")

        # handle variation in how the unnested column is represented
        if self._unnest_column:
            if self._unnest_column.node_type == NodeType.NESTED:
                self._unnest_column = self._unnest_column.centre
            # if we have a literal that's not a tuple, wrap it
            if self._unnest_column.node_type == NodeType.LITERAL and not isinstance(
                self._unnest_column.value, tuple
            ):
                self._unnest_column.value = tuple([self._unnest_column.value])

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> typing.Iterable:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore
        right_table = pyarrow.concat_tables(right_node.execute(), mode="default")  # type:ignore

        if self._unnest_column is None:
            yield from _cross_join(left_node, right_table)

        elif isinstance(self._unnest_column.value, tuple):
            yield from _cross_join_unnest_literal(
                morsels=left_node,
                source=self._unnest_column.value,
                target_column=self._unnest_target,
            )
        else:
            yield from _cross_join_unnest_column(
                morsels=left_node, source=self._unnest_column, target_column=self._unnest_target
            )
