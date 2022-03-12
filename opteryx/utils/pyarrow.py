
from typing import Iterable, List


def fetchmany(pages: Iterable, size: int = 5) -> List[dict]:  # type:ignore
    """
    This is the fastest way I've found to do this.

    The limitation code slows it down a little, but that's to be expected.
    """
    DEFAULT_CHUNK_SIZE = 100
    chunk_size = min(size, DEFAULT_CHUNK_SIZE)

    def _inner_row_reader():
        for page in pages:
            for batch in page.to_batches(max_chunksize=chunk_size):
                dict_batch = batch.to_pydict()
                for index in range(len(batch)):
                    yield {k: v[index] for k, v in dict_batch.items()}

    index = -1
    for index, row in enumerate(_inner_row_reader()):
        if index == size:
            return
        yield row

    if index < 0:
        yield {}


def fetchone(pages: Iterable) -> dict:
    return fetchmany(pages=pages, size=1).pop()


def fetchall(pages) -> List[dict]:
    return fetchmany(pages=pages, size=-1)


"""
Adapted from:
https://stackoverflow.com/questions/55546027/how-to-assign-arbitrary-metadata-to-pyarrow-table-parquet-columns
"""

def set_metadata(tbl, col_meta={}, tbl_meta={}):
    """
    Store table- and column-level metadata as json-encoded byte strings.

    Table-level metadata is stored in the table's schema.
    Column-level metadata is stored in the table columns' fields.

    To update the metadata, first new fields are created for all columns.
    Next a schema is created using the new fields and updated table metadata.
    Finally a new table is created by replacing the old one's schema, but
    without copying any data.

    Args:
        tbl (pyarrow.Table): The table to store metadata in
        col_meta: A json-serializable dictionary with column metadata in the form
            {
                'column_1': {'some': 'data', 'value': 1},
                'column_2': {'more': 'stuff', 'values': [1,2,3]}
            }
        tbl_meta: A json-serializable dictionary with table-level metadata.
    """
    import pyarrow as pa
    import json
    # Create updated column fields with new metadata
    if col_meta or tbl_meta:
        fields = []
        for col in tbl.schema.names:
            if col in col_meta:
                # Get updated column metadata
                metadata = tbl.field(col).metadata or {}
                for k, v in col_meta[col].items():
                    metadata[k] = json.dumps(v).encode('utf-8')
                # Update field with updated metadata
                fields.append(tbl.field(col).with_metadata(metadata))
            else:
                fields.append(tbl.field(col))
        
        # Get updated table metadata
        tbl_metadata = tbl.schema.metadata or {}
        for k, v in tbl_meta.items():
            if type(v)==bytes:
                tbl_metadata[k] = v
            else:
                tbl_metadata[k] = json.dumps(v).encode('utf-8')

        # Create new schema with updated field metadata and updated table metadata
        schema = pa.schema(fields, metadata=tbl_metadata)

        # With updated schema build new table (shouldn't copy data)
        # tbl = pa.Table.from_batches(tbl.to_batches(), schema)
        tbl = tbl.cast(schema)

    return tbl

def _decode_metadata(metadata):
    """
    Arrow stores metadata keys and values as bytes. We store "arbitrary" data as
    json-encoded strings (utf-8), which are here decoded into normal dict.
    """
    import json

    if not metadata:
        # None or {} are not decoded
        return metadata

    decoded = {}
    for k, v in metadata.items():
        key = k.decode('utf-8')
        val = json.loads(v.decode('utf-8'))
        decoded[key] = val
    return decoded


def get_table_metadata(tbl):
    """Get table metadata as dict."""
    return _decode_metadata(tbl.schema.metadata)

def get_column_metadata(tbl):
    """Get column metadata as dict."""
    return {col.name: _decode_metadata(col.field.metadata) for col in tbl.itercolumns()}

def get_metadata(tbl):
    """Get column and table metadata as dicts."""
    return get_column_metadata(tbl), get_table_metadata(tbl)