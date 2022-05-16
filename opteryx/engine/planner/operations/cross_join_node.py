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

This performs a CROSS JOIN - CROSS JOIN is not natively supported by
PyArrow so this is written out rather than calling the join() functions
"""
from typing import Iterable

import numpy
import pyarrow

from opteryx import config
from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.utils.columns import Columns


def _cartesian_product(*arrays):
    """
    Cartesian product of arrays creates every combination of the elements in the arrays
    """
    array_count = len(arrays)
    arr = numpy.empty(
        [len(array) for array in arrays] + [array_count], dtype=numpy.int64
    )
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
            (seq_1[pos : pos + size], seq_2[pos : pos + size])
            for pos in range(0, len(seq_1), size)
        )

    from opteryx.third_party.pyarrow_ops import align_tables

    if isinstance(left, pyarrow.Table):
        left = [left]

    right_columns = Columns(right)
    left_columns = None

    for left_page in left:

        if left_columns is None:
            left_columns = Columns(left_page)
            new_columns = left_columns + right_columns

        # we break this into small chunks, each cycle will have 100 * rows in the right table
        for left_block in left_page.to_batches(
            max_chunksize=config.INTERNAL_BATCH_SIZE
        ):

            # blocks don't have column_names, so we need to wrap in a table
            left_block = pyarrow.Table.from_batches(
                [left_block], schema=left_page.schema
            )

            # build two lists, 0 to num_rows for each table
            left_array = numpy.arange(left_block.num_rows, dtype=numpy.int64)
            right_array = numpy.arange(right.num_rows, dtype=numpy.int64)

            # build the cartesian product of the two lists
            left_align, right_align = _cartesian_product(left_array, right_array)

            # CROSS JOINs can create huge tables quickly, this is used to limit the
            # number of records we hold in memory at any time
            for left_chunk, right_chunk in _chunker(
                left_align, right_align, config.MAX_JOIN_SIZE
            ):

                # now build the resultant table
                table = align_tables(
                    left_block, right, left_chunk.flatten(), right_chunk.flatten()
                )
                yield new_columns.apply(table)


def _cross_join_unnest(left, column, alias):
    """
    This is a specific instance the CROSS JOIN, where instead of joining on another
    table, we're joining on a field in the current row.

    This means we need to read a row, create the dataset to join with, do the join
    repeat.

    This is done by collecting the values together and creating them into a new column.

    This column-based approach benchmarked roughly 33% faster than the row-based
    approach, where each row was read as a dictionary, new dictionaries created for
    each UNNESTed value and the dictionaries combined to a table.
    """

    if column[1] != TOKEN_TYPES.IDENTIFIER:
        raise NotImplementedError("Can only CROSS JOIN UNNEST on a field")

    if isinstance(left, pyarrow.Table):
        left = [left]

    metadata = None

    if alias is None:
        alias = f"UNNEST({column[0]})"

    for left_page in left:

        if metadata is None:
            metadata = Columns(left_page)
            metadata.add_column(alias)
            unnest_column = metadata.get_column_from_alias(column[0], only_one=True)

        batch_size = config.INTERNAL_BATCH_SIZE
        # we break this into small chunks otherwise we very quickly run into memory issues
        for left_block in left_page.to_batches(max_chunksize=batch_size):

            # Get the column we're going to UNNEST
            column_data = left_block[unnest_column]

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
            new_block = metadata.apply(new_block)
            yield new_block


class CrossJoinNode(BasePlanNode):
    """
    Implements a SQL CROSS JOIN and CROSS JOIN UNNEST
    """

    def __init__(self, statistics: QueryStatistics, **config):
        self._right_table = config.get("right_table")
        self._join_type = config.get("join_type", "CrossJoin")

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, data_pages: Iterable) -> Iterable:

        from opteryx.engine.planner.operations import DatasetReaderNode

        if isinstance(self._right_table, DatasetReaderNode):
            self._right_table = pyarrow.concat_tables(
                self._right_table.execute(None)
            )  # type:ignore

        if self._join_type == "CrossJoin":
            yield from _cross_join(data_pages, self._right_table)

        elif self._join_type == "CrossJoinUnnest":
            yield from _cross_join_unnest(
                left=data_pages,
                column=self._right_table[1][1][0],
                alias=self._right_table[0],
            )
