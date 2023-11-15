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
from orjson import dumps
from orjson import loads

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


def coerce_columns(table, column_names):
    """convert numeric types to a common type to allow comparisons"""
    # get the column we're coercing
    my_schema = table.schema

    if not isinstance(column_names, list):
        column_names = [column_names]

    for column_name in column_names:
        index = table.column_names.index(column_name)
        column = my_schema.field(column_name)

        # if it's numeric, and not already the type we want, convert it
        if str(column.type) in ("int64", "double"):
            column = column.with_type(pyarrow.float64())
            my_schema = my_schema.set(index, pyarrow.field(column_name, pyarrow.float64()))
            table = table.cast(target_schema=my_schema)

    return table


def remove_null_columns(table):
    removed = []
    kept = []
    for column in table.column_names:
        column_data = table.column(column)
        if str(column_data.type) == "null":  # pragma: no cover
            removed.append(column)
        else:
            kept.append(column)

    return removed, table.select(kept)


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
