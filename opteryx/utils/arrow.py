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
This module contains support functions for working with PyArrow
"""

from typing import Iterator
from typing import Optional

import pyarrow

INTERNAL_BATCH_SIZE = 500


def limit_records(
    morsels: Iterator[pyarrow.Table], limit: Optional[int] = None, offset: int = 0
) -> Optional[Iterator[pyarrow.Table]]:
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

        if not at_least_one:
            # make sure we return at least an empty morsel from this function
            yield morsel.slice(offset=0, length=0)
            at_least_one = True

        remaining_rows -= morsel.num_rows
        if remaining_rows <= 0:
            break

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
        name: projection_column.name
        for projection_column in columns
        for name in projection_column.all_names
    }

    columns_to_keep = [
        schema_column for schema_column in schema_columns if schema_column in name_mapping
    ]
    column_names = [name_mapping[schema_column] for schema_column in columns_to_keep]

    table = table.select(columns_to_keep)
    return table.rename_columns(column_names)


def align_tables(source_table, append_table, source_indices, append_indices):
    # If either source_indices or append_indices is empty, return the source_table taken with source_indices immediately
    if len(source_indices) == 0 or len(append_indices) == 0:
        # Combine schemas from both tables
        combined_schema = pyarrow.schema([])
        for field in source_table.schema:
            combined_schema = combined_schema.append(field)
        for field in append_table.schema:
            if field.name not in combined_schema.names:
                combined_schema = combined_schema.append(field)

        # Create and return an empty table with the combined schema
        empty_arrays = [pyarrow.array([]) for field in combined_schema]
        return pyarrow.Table.from_arrays(empty_arrays, schema=combined_schema)

    if all(s is None for s in source_indices):
        empty_arrays = [
            pyarrow.nulls(len(source_indices), type=field.type) for field in source_table.schema
        ]
        aligned_table = pyarrow.Table.from_arrays(empty_arrays, schema=source_table.schema)
    else:
        # Take the rows from source_table at the specified source_indices
        aligned_table = source_table.take(source_indices)

    # Create a set of column names from the source table for efficient existence checking
    source_column_names = set(source_table.column_names)

    # Iterate through the column names of append_table
    for column_name in append_table.column_names:
        # If the column_name is not found in source_column_names
        if column_name not in source_column_names:
            # Append the column from append_table to aligned_table, taking the elements at the specified append_indices
            aligned_table = aligned_table.append_column(
                column_name, append_table.column(column_name).take(append_indices)
            )

    # Return the aligned table
    return aligned_table
