# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unnest Join Node

This is a SQL Query Execution Plan Node.

This implements a CROSS JOIN UNNEST, this isn't really a JOIN in that it doesn't join two tables
together, but it does unnest a column in a table and repeat the rows in the table for each value.
"""

from typing import Generator
from typing import Set
from typing import Tuple

import numpy
import pyarrow
from orso.schema import FlatColumn

from opteryx import EOS
from opteryx.managers.expression import NodeType
from opteryx.models import LogicalColumn
from opteryx.models import QueryProperties
from opteryx.third_party.abseil.containers import FlatHashSet

from . import BasePlanNode

INTERNAL_BATCH_SIZE: int = 10000  # config
MAX_JOIN_SIZE: int = 1000  # config
MORSEL_SIZE_BYTES: int = 16 * 1024 * 1024
CROSS_JOIN_UNNEST_BATCH_SIZE = 10000


def _cross_join_unnest_column(
    *,
    morsel: pyarrow.Table = None,
    source: LogicalColumn = None,
    target_column: FlatColumn = None,
    conditions: Set = None,
    distinct: bool = False,
    single_column: bool = False,
    hash_set=None,
) -> pyarrow.Table:
    """
    Perform a cross join on an unnested column of pyarrow tables.

    Args:
        morsels: An iterable of `pyarrow.Table` objects to be cross joined.
        source: The source node indicating the column.
        target_column: The column to be unnested.

    Returns:
        A generator that yields the resulting `pyarrow.Table` objects.
    """
    from opteryx.compiled.joins.cross_join import build_filtered_rows_indices_and_column
    from opteryx.compiled.joins.cross_join import build_rows_indices_and_column
    from opteryx.compiled.joins.cross_join import list_distinct
    from opteryx.compiled.joins.cross_join import numpy_build_filtered_rows_indices_and_column
    from opteryx.compiled.joins.cross_join import numpy_build_rows_indices_and_column

    batch_size: int = INTERNAL_BATCH_SIZE
    at_least_once = False
    single_column_collector = []

    # Break the morsel into batches to avoid memory issues
    for left_block in morsel.to_batches(max_chunksize=batch_size):
        new_block = None
        # Fetch the data of the column to be unnested
        column_data = left_block[source.schema_column.identity]

        # Filter out null values
        valid_offsets = column_data.is_valid()
        if valid_offsets.true_count == 0:
            continue
        column_data = column_data.filter(valid_offsets)
        left_block = left_block.filter(valid_offsets)

        # Build indices and new column data
        if conditions is None:
            if hasattr(column_data.type, "value_type") and (
                column_data.type.value_type == pyarrow.string()
                or column_data.type.value_type == pyarrow.binary()
            ):
                # optimized version for string and binary columns
                indices, new_column_data = build_rows_indices_and_column(column_data)
            else:
                # fallback to numpy version
                indices, new_column_data = numpy_build_rows_indices_and_column(
                    column_data.to_numpy(False)
                )
        else:
            if hasattr(column_data.type, "value_type") and (
                column_data.type.value_type == pyarrow.string()
                or column_data.type.value_type == pyarrow.binary()
            ):
                indices, new_column_data = build_filtered_rows_indices_and_column(
                    column_data, conditions
                )
            else:
                indices, new_column_data = numpy_build_filtered_rows_indices_and_column(
                    column_data.to_numpy(False), conditions
                )

        if single_column and distinct and indices.size > 0:
            # if the unnest target is the only field in the SELECT and we're DISTINCTING
            indices = numpy.array(indices, dtype=numpy.int64)
            new_column_data, indices, hash_set = list_distinct(new_column_data, indices, hash_set)

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
                    arrow_array = pyarrow.array(single_column_collector)
                    if arrow_array.type != target_column.arrow_field.type:
                        arrow_array = arrow_array.cast(target_column.arrow_field.type)
                    new_block = pyarrow.Table.from_arrays([arrow_array], schema=schema)
                    single_column_collector.clear()
                    del arrow_array
                    yield new_block
                    at_least_once = True
            else:
                # Rebuild the block with the new column data if we have any rows to build for

                total_rows = indices.size  # Both arrays have the same length
                block_size = MORSEL_SIZE_BYTES / (left_block.nbytes / left_block.num_rows)
                block_size = int(block_size / 1000) * 1000

                for start_block in range(0, total_rows, block_size):
                    # Compute the end index for the current chunk
                    end_block = min(start_block + block_size, total_rows)

                    # Slice the current chunk of indices and new_column_data
                    indices_chunk = indices[start_block:end_block]
                    new_column_data_chunk = new_column_data[start_block:end_block]

                    # Create a new block using the chunk of indices
                    indices_chunk = numpy.array(indices_chunk, dtype=numpy.int64)
                    new_block = left_block.take(indices_chunk)
                    new_block = pyarrow.Table.from_batches([new_block], schema=morsel.schema)

                    # Append the corresponding chunk of new_column_data to the block
                    new_block = new_block.append_column(
                        target_column.identity, pyarrow.array(new_column_data_chunk)
                    )

                    yield new_block
                    at_least_once = True

    if single_column_collector:
        schema = pyarrow.schema(
            [pyarrow.field(name=target_column.identity, type=target_column.arrow_field.type)]
        )
        arrow_array = pyarrow.array(single_column_collector)
        if arrow_array.type != target_column.arrow_field.type:
            arrow_array = arrow_array.cast(target_column.arrow_field.type)
        new_block = pyarrow.Table.from_arrays([arrow_array], schema=schema)
        yield new_block
        at_least_once = True

    if not at_least_once:
        # Create an empty table with the new schema
        schema = morsel.schema
        new_column = pyarrow.field(target_column.identity, pyarrow.string())
        new_schema = pyarrow.schema(list(schema) + [new_column])
        new_block = pyarrow.Table.from_batches([], schema=new_schema)
        yield new_block


def _cross_join_unnest_literal(
    morsel: pyarrow.Table, source: Tuple, target_column: FlatColumn
) -> Generator[pyarrow.Table, None, None]:
    joined_list_size = len(source)

    # Break the morsel into batches to avoid memory issues
    for left_block in morsel.to_batches(max_chunksize=INTERNAL_BATCH_SIZE):
        left_block = pyarrow.Table.from_batches([left_block], schema=morsel.schema)
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


class UnnestJoinNode(BasePlanNode):
    """
    Implements CROSS JOIN UNNEST
    """

    is_stateless = True

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # do we have unnest details?
        self._unnest_column = parameters.get("unnest_column")
        self._unnest_target = parameters.get("unnest_target").schema_column
        self._filters = parameters.get("filters")
        self._distinct = parameters.get("distinct", False)

        # handle variation in how the unnested column is represented
        if self._unnest_column.node_type == NodeType.NESTED:
            self._unnest_column = self._unnest_column.centre

        # if we have a literal that's not a tuple, wrap it
        if self._unnest_column.node_type == NodeType.LITERAL and not isinstance(
            self._unnest_column.value, tuple
        ):
            self._unnest_column.value = tuple([self._unnest_column.value])

        self._single_column = parameters.get("pre_update_columns", set()) == {
            self._unnest_target.identity,
        }

        self.hash_set = FlatHashSet()

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        filters = ""
        if self._filters:
            filters = f"({self._unnest_target.name} IN ({', '.join(self._filters)}))"
        return f"CROSS JOIN {filters}"

    def execute(self, morsel: pyarrow.Table, join_leg: str = None) -> pyarrow.Table:
        if morsel == EOS:
            yield EOS
            return
        if isinstance(self._unnest_column.value, tuple):
            yield from _cross_join_unnest_literal(
                morsel=morsel,
                source=self._unnest_column.value,
                target_column=self._unnest_target,
            )
            return

        yield from _cross_join_unnest_column(
            morsel=morsel,
            source=self._unnest_column,
            target_column=self._unnest_target,
            conditions=self._filters,
            hash_set=self.hash_set,
            distinct=self._distinct,
            single_column=self._single_column,
        )
        return
