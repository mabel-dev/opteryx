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
from pyarrow import Table
from pyarrow import array
from pyarrow import nulls


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


def post_read_projector(table: pyarrow.Table, columns: list) -> pyarrow.Table:
    """
    This is the near-read projection for data sources that the projection can't be
    done as part of the read.
    """
    if not columns:
        # this should happen when there's no relation in the query
        return table

    table_cols = table.column_names
    target_names = [c.schema_column.name for c in columns]

    if set(table_cols) == set(target_names):
        return table  # nothing to do

    schema_columns = set(table_cols)

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

    This function was originally copied from
    https://github.com/TomScheffers/pyarrow_ops/blob/main/pyarrow_ops/join.py

    Parameters:
        source_table: The pyarrow.Table to align.
        append_table: The pyarrow.Table to align with the source_table.
        source_indices: The indices to take from the source_table.
        append_indices: The indices to take from the append_table.

    Returns:
        A pyarrow.Table with aligned columns and data.
    """
    len_src = len(source_indices)
    len_app = len(append_indices)

    if len_src == 0 or len_app == 0:
        combined_fields = [
            *source_table.schema,
            *[f for f in append_table.schema if f.name not in source_table.schema.names],
        ]
        return Table.from_arrays(
            [nulls(0, type=f.type) for f in combined_fields], schema=pyarrow.schema(combined_fields)
        )

    if not isinstance(source_indices, pyarrow.Array):
        source_indices = array(source_indices, type=pyarrow.int64())
    if not isinstance(append_indices, pyarrow.Array):
        append_indices = array(append_indices, type=pyarrow.int64())

    if source_indices.null_count == len_src:
        src_cols = [nulls(len_src, type=f.type) for f in source_table.schema]
    else:
        src_cols = [col.take(source_indices) for col in source_table.columns]

    append_all_nulls = append_indices.null_count == len_app
    source_names = set(source_table.schema.names)

    new_cols = []
    new_fields = []

    for name, field in zip(append_table.schema.names, append_table.schema):
        if name not in source_names:
            if append_all_nulls:
                col = nulls(len_src, type=field.type)
            else:
                col = append_table.column(name).take(append_indices)
            new_cols.append(col)
            new_fields.append(field)

    return Table.from_arrays(
        src_cols + new_cols, schema=pyarrow.schema(list(source_table.schema) + new_fields)
    )
