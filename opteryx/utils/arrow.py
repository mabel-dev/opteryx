from typing import Iterable, List


def fetchmany(pages, limit: int = 1000):

    from pyarrow import Table

    from opteryx.utils.columns import Columns

    if pages is None:
        return []

    if isinstance(pages, Table):
        pages = (pages,)

    default_chunk_size = 1000
    chunk_size = min(limit, default_chunk_size)
    if chunk_size < 0:
        chunk_size = default_chunk_size

    def _inner_row_reader():

        column_names = None

        for page in pages:

            if column_names is None:
                columns = Columns(page)
                preferred_names = columns.preferred_column_names
                column_names = []
                for col in page.column_names:
                    column_names.append([c for a, c in preferred_names if a == col][0])

            page = page.rename_columns(column_names)

            for batch in page.to_batches(max_chunksize=chunk_size):
                yield from batch.to_pylist()

    index = -1
    for index, row in enumerate(_inner_row_reader()):
        if index == limit:
            return
        yield row

    if index < 0:
        yield {}


def fetchone(pages: Iterable) -> dict:
    return next(fetchmany(pages=pages, limit=1), None)


def fetchall(pages) -> List[dict]:
    return fetchmany(pages=pages, limit=-1)


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
    import pyarrow as pa
    from orjson import dumps

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
        schema = pa.schema(fields, metadata=tbl_metadata)
        # With updated schema build new table (shouldn't copy data)
        table = table.cast(schema)

    return table


def _decode_metadata(metadata):
    """
    Arrow stores metadata keys and values as bytes. We store "arbitrary" data as
    json-encoded strings (utf-8), which are here decoded into normal dict.
    """
    from orjson import loads

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
