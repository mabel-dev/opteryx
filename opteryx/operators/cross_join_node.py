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

INTERNAL_BATCH_SIZE: int = 7500  # config
MAX_JOIN_SIZE: int = 1000  # config
MORSEL_SIZE_BYTES: int = 32 * 1024 * 1024


def _cross_join_unnest_column(
    morsels: BasePlanNode = None,
    source: Node = None,
    target_column: FlatColumn = None,
    conditions: Set = None,
    statistics=None,
    distinct: bool = False,
    single_column: bool = False,
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
    from opteryx.compiled.structures import HashSet
    from opteryx.compiled.structures import list_distinct

    hash_set = HashSet()

    # Check if the source node type is an identifier, raise error otherwise
    if source.node_type != NodeType.IDENTIFIER:
        raise NotImplementedError("Can only CROSS JOIN UNNEST on a column")

    column_type = None
    batch_size: int = INTERNAL_BATCH_SIZE
    at_least_once = False
    single_column_collector = []

    # Loop through each morsel from the morsels execution
    for left_morsel in morsels.execute():
        start = time.monotonic_ns()
        # Break the morsel into batches to avoid memory issues
        for left_block in left_morsel.to_batches(max_chunksize=batch_size):
            new_block = None
            # Fetch the data of the column to be unnested
            column_data = left_block[source.schema_column.identity]

            # Filter out null values
            valid_offsets = column_data.is_valid()
            column_data = column_data.drop_null()
            if len(column_data) == 0:
                continue
            left_block = left_block.filter(valid_offsets)

            # Set column_type if it hasn't been determined already
            if column_type is None:
                column_type = column_data.type.value_type

            # Build indices and new column data
            if conditions is None:
                indices, new_column_data = build_rows_indices_and_column(
                    column_data.to_numpy(False)
                )
            else:
                indices, new_column_data = build_filtered_rows_indices_and_column(
                    column_data.to_numpy(False), conditions
                )

            if single_column and distinct and indices.size > 0:
                # if the unnest target is the only field in the SELECT and we're DISTINCTING
                new_column_data, indices, hash_set = list_distinct(
                    new_column_data, indices, hash_set
                )

            if len(indices) > 0:
                if single_column:
                    single_column_collector.extend(new_column_data)
                    if len(single_column_collector) > INTERNAL_BATCH_SIZE:
                        schema = pyarrow.schema(
                            [
                                pyarrow.field(
                                    name=target_column.identity, type=target_column.arrow_field.type
                                )
                            ]
                        )
                        new_block = pyarrow.Table.from_arrays(
                            [single_column_collector], schema=schema
                        )
                        single_column_collector.clear()
                else:
                    # Rebuild the block with the new column data if we have any rows to build for
                    new_block = left_block.take(indices)
                    new_block = pyarrow.Table.from_batches([new_block], schema=left_morsel.schema)
                    new_block = new_block.append_column(target_column.identity, [new_column_data])

                statistics.time_cross_join_unnest += time.monotonic_ns() - start
                if new_block and new_block.num_rows > 0:
                    yield new_block
                    at_least_once = True
                start = time.monotonic_ns()

    if single_column_collector:
        schema = pyarrow.schema(
            [pyarrow.field(name=target_column.identity, type=target_column.arrow_field.type)]
        )
        new_block = pyarrow.Table.from_arrays([single_column_collector], schema=schema)
        yield new_block
        at_least_once = True

    if not at_least_once:
        # Create an empty table with the new schema
        schema = left_morsel.schema
        new_column = pyarrow.field(target_column.identity, pyarrow.string())
        new_schema = pyarrow.schema(list(schema) + [new_column])
        new_block = pyarrow.Table.from_batches([], schema=new_schema)
        yield new_block


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

    at_least_once = False
    left_schema = None
    right_schema = right.schema

    for left_morsel in left.execute():
        if left_schema is None:
            left_schema = left_morsel.schema
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
                at_least_once = True
                start = time.monotonic_ns()

    if not at_least_once:
        fields = [pyarrow.field(name=f.name, type=f.type) for f in right_schema] + [
            pyarrow.field(name=f.name, type=f.type) for f in left_schema
        ]
        combined_schemas = pyarrow.schema(fields)
        yield pyarrow.Table.from_arrays(
            [pyarrow.array([]) for _ in combined_schemas], schema=combined_schemas
        )


@dataclass
class CrossJoinDataObject(BasePlanDataObject):
    source: str = None
    _unnest_column: str = None
    _unnest_target: str = None
    _filters: str = None
    _distinct: bool = False


class CrossJoinNode(BasePlanNode):
    """
    Implements a SQL CROSS JOIN
    """

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

        self.source = config.get("column")

        self._left_relation = config.get("left_relation_names")
        self._right_relation = config.get("right_relation_names")

        # do we have unnest details?
        self._unnest_column = config.get("unnest_column")
        self._unnest_target = config.get("unnest_target")
        self._filters = config.get("filters")
        self._distinct = config.get("distinct", False)

        # handle variation in how the unnested column is represented
        if self._unnest_column:
            if self._unnest_column.node_type == NodeType.NESTED:
                self._unnest_column = self._unnest_column.centre
            # if we have a literal that's not a tuple, wrap it
            if self._unnest_column.node_type == NodeType.LITERAL and not isinstance(
                self._unnest_column.value, tuple
            ):
                self._unnest_column.value = tuple([self._unnest_column.value])

            self._single_column = config.get("pre_update_columns", set()) == {
                self._unnest_target.identity,
            }

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        filters = ""
        if self._filters:
            filters = f"({self._unnest_target.name} IN ({', '.join(self._filters)}))"
        return f"CROSS JOIN {filters}"

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        if self._unnest_column is None:
            right_table = pyarrow.concat_tables(right_node.execute(), promote_options="none")  # type:ignore
            yield from _cross_join(left_node, right_table, self.statistics)

        elif isinstance(self._unnest_column.value, tuple):
            yield from _cross_join_unnest_literal(
                morsels=left_node,
                source=self._unnest_column.value,
                target_column=self._unnest_target,
                statistics=self.statistics,
            )
        else:
            if hasattr(left_node, "function") and left_node.function == "UNNEST":
                left_node = right_node
            yield from _cross_join_unnest_column(
                morsels=left_node,
                source=self._unnest_column,
                target_column=self._unnest_target,
                conditions=self._filters,
                statistics=self.statistics,
                distinct=self._distinct,
                single_column=self._single_column,
            )
