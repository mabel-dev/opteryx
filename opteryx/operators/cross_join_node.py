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
from opteryx.managers.expression import NodeType
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


def _cross_join_unnest(
    left: pyarrow.Table, column, alias: str = None
) -> typing.Generator[pyarrow.Table, None, None]:
    """
    Cross join on an unnested column. This function reads a row, creates a new column by unnesting
    a column in that row, and then repeats the row for each unnested value. It then returns the
    resulting table.

    Args:
        left: An iterator of `pyarrow.Table` objects to cross join on.
        column: The column to unnest.
        alias: An optional string to use as the column name for the unnested column.

    Returns:
        A generator that yields the resulting `pyarrow.Table` objects.
    """

    if column.node_type != NodeType.IDENTIFIER:
        raise ValueError("The `column` argument must be a valid identifier.")

    if column.node_type != NodeType.IDENTIFIER:
        raise NotImplementedError("Can only CROSS JOIN UNNEST on a field")

    metadata = None
    column_type = None

    if alias is None:
        alias = f"UNNEST({column.value})"

    for left_morsel in left.execute():
        if metadata is None:
            metadata = Columns(left_morsel)
            metadata.add_column(alias)
            unnest_column = metadata.get_column_from_alias(column.value, only_one=True)

        # we break this into small chunks otherwise we very quickly run into memory issues
        for left_block in left_morsel.to_batches(max_chunksize=INTERNAL_BATCH_SIZE):
            # Get the column we're going to UNNEST
            column_data = left_block[unnest_column]
            if column_type is None:
                column_type = column_data.type.value_type

            # Create a list of indexes, this will look something like this:
            # [1,1,1,2,2,2,3,3,3]
            # Where the number of times a number is repeated, is the length of the list
            # we're going to UNNEST for that row
            indexes = []
            new_column = []
            for i, value in enumerate(column_data):
                # if the value isn't valid, we can't UNNEST it
                if value.is_valid and len(value.values) != 0:
                    indexes.extend([i] * len(value))
                    new_column.extend(value)
                else:
                    indexes.append(i)
                    new_column.append(None)

            if len(indexes) == 0:
                continue

            # Strings need special treatment to avoid them being coerced into a list
            # of characters
            new_column = [(v.as_py() if hasattr(v, "as_py") else v for v in new_column)]

            # Using the indexes above, repeat the rows of the source data
            new_block = left_block.take(indexes)
            # We can't append columns to batches, so we need to convert to a table
            new_block = pyarrow.Table.from_batches([new_block])
            # Append the column we created above, to the table with the repeated rows
            new_block = pyarrow.Table.append_column(new_block, alias, new_column)

            # if the entire columns is nulls, the schema won't match
            column_index = new_block.column_names.index(alias)
            schema = new_block.schema
            column = schema.field(column_index)
            if column.type != column_type:
                schema = schema.set(column_index, pyarrow.field(alias, column_type))
                new_block = new_block.cast(target_schema=schema)

            new_block = metadata.apply(new_block)
            yield new_block


class CrossJoinNode(BasePlanNode):
    """
    Implements a SQL CROSS JOIN and CROSS JOIN UNNEST
    """

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._right_table = config.get("right_table")
        self._join_type = config.get("join_type", "CrossJoin")

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        if self._join_type == "CrossJoinUnnest":
            return "UNNEST()"
        return ""

    def execute(self) -> typing.Iterable:
        if len(self._producers) != 2:  # pragma: no cover
            raise SqlError(f"{self.name} expects two producers")

        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        if self._join_type == "CrossJoin":
            self._right_table = pyarrow.concat_tables(
                right_node.execute(), promote=True
            )  # type:ignore

            yield from _cross_join(left_node, self._right_table)

        elif self._join_type == "CrossJoinUnnest":
            function = right_node.dataset["function"]
            args = right_node.dataset["args"]

            if function != "unnest":
                raise SqlError(f"I was expecting 'UNNEST' but I got `{function}`")

            yield from _cross_join_unnest(left=left_node, column=args[0], alias=right_node.alias)
