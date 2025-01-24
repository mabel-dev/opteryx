# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This module contains support functions for working with PyArrow
"""

from typing import Iterator
from typing import List
from typing import Optional
from typing import Union

import pyarrow

INTERNAL_BATCH_SIZE = 500


def limit_records(
    morsels: Iterator[pyarrow.Table], limit: Optional[int] = None, offset: int = 0
) -> Optional[Iterator[pyarrow.Table]]:  # pragma: no cover
    """
    Cycle over an iterable of morsels, limiting the response to a given
    number of records with an optional offset.
    """
    remaining_rows = limit if limit is not None else float("inf")
    rows_left_to_skip = max(0, offset)
    at_least_one = False

    for morsel in morsels:
        if rows_left_to_skip > 0:
            if rows_left_to_skip >= morsel.num_rows:
                rows_left_to_skip -= morsel.num_rows
                continue
            else:
                morsel = morsel.slice(
                    offset=rows_left_to_skip, length=morsel.num_rows - rows_left_to_skip
                )
                rows_left_to_skip = 0

        if morsel.num_rows > 0:
            if morsel.num_rows < remaining_rows:
                yield morsel
                at_least_one = True
            else:
                yield morsel.slice(offset=0, length=remaining_rows)
                at_least_one = True

        remaining_rows -= morsel.num_rows
        if remaining_rows <= 0:
            break

    if not at_least_one:
        # make sure we return at least an empty morsel from this function
        yield morsel.slice(offset=0, length=0)
        at_least_one = True

    if not remaining_rows:
        return None


def restore_null_columns(removed, table):
    for column in removed:  # pragma: no cover
        table = table.append_column(column, pyarrow.array([None] * table.num_rows))
    return table


def post_read_projector(table: pyarrow.Table, columns: list) -> pyarrow.Table:
    """
    This is the near-read projection for data sources that the projection can't be
    done as part of the read.
    """
    if not columns:
        # this should happen when there's no relation in the query
        return table

    schema_columns = set(table.column_names)

    # Using a dictionary to map all_names to the projection_column's name for quick lookup
    name_mapping = {
        name: projection_column.schema_column.name
        for projection_column in columns
        for name in projection_column.schema_column.all_names
    }

    columns_to_keep = [
        schema_column for schema_column in schema_columns if schema_column in name_mapping
    ]
    column_names = [name_mapping[schema_column] for schema_column in columns_to_keep]

    table = table.select(columns_to_keep)
    return table.rename_columns(column_names)


def align_tables(
    source_table: pyarrow.Table,
    append_table: pyarrow.Table,
    source_indices: Union[List[int], pyarrow.Array],
    append_indices: Union[List[int], pyarrow.Array],
) -> pyarrow.Table:
    """
    Aligns two tables based on provided indices, ensuring that the resulting table
    contains columns from both source_table and append_table.

    Parameters:
        source_table: The pyarrow.Table to align.
        append_table: The pyarrow.Table to align with the source_table.
        source_indices: The indices to take from the source_table.
        append_indices: The indices to take from the append_table.

    Returns:
        A pyarrow.Table with aligned columns and data.
    """
    len_source_indices = len(source_indices)
    len_append_indices = len(append_indices)

    # If either source_indices or append_indices is empty, return an empty table with a combined schema
    if len_source_indices == 0 or len_append_indices == 0:
        combined_schema = pyarrow.schema(
            [
                *source_table.schema,
                *[
                    field
                    for field in append_table.schema
                    if field.name not in source_table.schema.names
                ],
            ]
        )
        empty_arrays = [pyarrow.array([]) for field in combined_schema]
        return pyarrow.Table.from_arrays(empty_arrays, schema=combined_schema)

    # Convert indices to PyArrow arrays for efficient null checking
    if not isinstance(source_indices, pyarrow.Array):
        source_indices = pyarrow.array(source_indices, type=pyarrow.int64())
    if not isinstance(append_indices, pyarrow.Array):
        append_indices = pyarrow.array(append_indices, type=pyarrow.int64())

    # Check if all source_indices are nulls
    if source_indices.null_count == len_source_indices:
        null_columns = [
            pyarrow.nulls(len_source_indices, type=field.type) for field in source_table.schema
        ]
        aligned_table = pyarrow.Table.from_arrays(null_columns, schema=source_table.schema)
    else:
        # Take rows from source_table based on source_indices
        aligned_table = source_table.take(source_indices)

    # Set of column names from source_table for quick lookup
    source_column_names = set(source_table.column_names)

    # Check if all append_indices are nulls
    append_is_all_nulls = append_indices.null_count == len_append_indices

    new_columns = []
    new_fields = []

    # Collect all columns that need to be appended in one go
    for column_name, column_field in zip(append_table.column_names, append_table.schema):
        if column_name not in source_column_names:
            if append_is_all_nulls:
                # Create a column of nulls if all append indices are null
                null_column = pyarrow.nulls(len(aligned_table), type=column_field.type)
                new_columns.append(null_column)
            else:
                # Take the corresponding rows from append_table
                column_data = append_table.column(column_name).take(append_indices)
                new_columns.append(column_data)
            new_fields.append(column_field)

    # Combine original and new columns
    aligned_columns = aligned_table.columns + new_columns
    combined_schema = pyarrow.schema([*source_table.schema, *new_fields])

    # Create the final aligned table with all columns at once
    aligned_table = pyarrow.Table.from_arrays(aligned_columns, schema=combined_schema)

    return aligned_table
