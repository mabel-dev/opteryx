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
from typing import Iterable, List
from pyarrow import Table

import pyarrow

from orjson import dumps, loads

INTERNAL_BATCH_SIZE = 500


def fetchmany(pages, limit: int = 1000, as_dicts: bool = False):
    """fetch records from a Table as Python Dicts"""
    from opteryx.models.columns import Columns  # circular imports

    if pages is None:
        return []

    if isinstance(pages, Table):
        pages = (pages,)

    chunk_size = min(limit, INTERNAL_BATCH_SIZE)
    if chunk_size < 0:
        chunk_size = INTERNAL_BATCH_SIZE

    def _inner_row_reader():

        column_names = None
        schema = None

        for page in pages:

            if column_names is None:
                schema = page.schema
                columns = Columns(page)
                preferred_names = columns.preferred_column_names
                column_names = []
                for col in page.column_names:
                    column_names.append([c for a, c in preferred_names if a == col][0])

            page, schema = normalize_to_schema(page, schema)
            page = page.rename_columns(column_names)

            for batch in page.to_batches(max_chunksize=chunk_size):
                if as_dicts:
                    yield from batch.to_pylist()
                else:
                    yield from [tuple(tpl.values()) for tpl in batch.to_pylist()]

    index = -1
    for index, row in enumerate(_inner_row_reader()):
        if index == limit:
            return
        yield row

    if index < 0:
        if as_dicts:
            yield {}
        else:
            yield tuple()


def fetchone(pages: Iterable, as_dicts: bool = False) -> dict:
    return next(fetchmany(pages=pages, limit=1, as_dicts=as_dicts), None)


def fetchall(pages, as_dicts: bool = False) -> List[dict]:
    return fetchmany(pages=pages, limit=-1, as_dicts=as_dicts)


def limit_records(data_pages, limit):
    """
    Cycle over an iterable of pages, limiting the response to a given
    number of records.
    """
    result_set = []
    row_count = 0
    page = None

    # if we don't actually have a limit set, just return
    if limit is None:
        return pyarrow.concat_tables(data_pages, promote=True)

    for page in data_pages:
        if page.num_rows > 0:
            row_count += page.num_rows
            result_set.append(page)
            if row_count > limit:  # type:ignore
                break

    if len(result_set) == 0:
        return page
    else:
        return pyarrow.concat_tables(result_set, promote=True).slice(
            offset=0, length=limit
        )


def as_arrow(pages, limit: int = None):
    """return a result set a a pyarrow table"""
    # cicular imports
    from opteryx.models import Columns
    from opteryx.utils import peak

    first, pages = peak(pages)
    if first is not None:
        merged = limit_records(pages, limit)
        columns = Columns(merged)
        preferred_names = columns.preferred_column_names
        column_names = []
        for col in merged.column_names:
            column_names.append([c for a, c in preferred_names if a == col][0])

        return merged.rename_columns(column_names)
    return set()


# Adapted from:
# https://stackoverflow.com/questions/55546027/how-to-assign-arbitrary-metadata-to-pyarrow-table-parquet-columns


def set_metadata(table, table_metadata=None, column_metadata=None):
    """
    Store table-level metadata as json-encoded byte strings.

    Table-level metadata is stored in the table's schema.

    parameters:
        table: pyarrow.Table
            The table to store metadata in
        col_meta: dict
            A json-serializable dictionary with column metadata in the form
            {
                'column_1': {'some': 'data', 'value': 1},
                'column_2': {'more': 'stuff', 'values': [1,2,3]}
            }
        tbl_meta: dict
            A json-serializable dictionary with table-level metadata.
    """

    # Create updated column fields with new metadata
    if table_metadata or column_metadata:

        fields = []
        for name in table.schema.names:
            col = table.field(name)
            if col.name in column_metadata:
                # Get updated column metadata
                metadata = col.metadata or {}
                for k, v in column_metadata[name].items():
                    if isinstance(k, str):
                        k = k.encode()
                    metadata[k] = dumps(v)
                # Update field with updated metadata
                fields.append(col.with_metadata(metadata))
            else:
                fields.append(col)

        # Get updated table metadata
        tbl_metadata = table.schema.metadata or {}
        if table_metadata:
            for k, v in table_metadata.items():
                if isinstance(v, bytes):
                    tbl_metadata[k] = v
                else:
                    tbl_metadata[k] = dumps(v)

        # Create new schema with updated table metadata
        schema = pyarrow.schema(fields, metadata=tbl_metadata)
        # With updated schema build new table (shouldn't copy data)
        table = table.cast(schema)

    return table


def _decode_metadata(metadata):
    """
    Arrow stores metadata keys and values as bytes. We store "arbitrary" data as
    json-encoded strings (utf-8), which are here decoded into normal dict.
    """

    if not metadata:
        # None or {} are not decoded
        return {}

    decoded = {}
    for key, value in metadata.items():
        key = key.decode("utf-8")
        val = loads(value)
        decoded[key] = val
    return decoded


def table_metadata(tbl):
    """Get table metadata as dict."""
    return _decode_metadata(tbl.schema.metadata)


def column_metadata(tbl):
    """Get column metadata as dict."""
    return {col: _decode_metadata(tbl.field(col).metadata) for col in tbl.schema.names}


def get_metadata(tbl):
    """Get column and table metadata as dicts."""
    return column_metadata(tbl), table_metadata(tbl)


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
            my_schema = my_schema.set(
                index, pyarrow.field(column_name, pyarrow.float64())
            )
            table = table.cast(target_schema=my_schema)

    return table


def normalize_to_schema(table, schema):
    """
    Ensure all of the collected pages match the same schema, because of the way we read
    data, this is to match the first page. We ensure they match by adding empty columns
    when columns are missing, or removing excess columns.
    """
    # if we've never run before, collect the schema and return
    if schema is None:
        schema = table.schema
        return table, schema

    # remove unwanted columns
    table = table.select([name for name in schema.names if name in table.schema.names])

    # add missing columns
    for column in [name for name in schema.names if name not in table.schema.names]:
        table = table.append_column(column, [[None] * table.num_rows])

    # cast mismtched columns
    # the orders may be different - so build hash tables for comparing
    first_types = dict(zip(schema.names, schema.types))
    this_types = dict(zip(table.schema.names, table.schema.types))

    for column in schema.names:
        if (
            first_types[column] != this_types[column]
            and first_types[column] != pyarrow.null()
        ):
            index = table.column_names.index(column)
            my_schema = table.schema.set(
                index, pyarrow.field(column, first_types[column])
            )

            table = table.cast(target_schema=my_schema)

    return table, schema
