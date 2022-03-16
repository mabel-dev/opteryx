
from typing import Iterable, List


def fetchmany(pages, limit: int = 1000):
    DEFAULT_CHUNK_SIZE = 1000
    chunk_size = min(limit, DEFAULT_CHUNK_SIZE)
    if chunk_size < 0:
        chunk_size = DEFAULT_CHUNK_SIZE

    def _inner_row_reader():
        for page in pages:
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
    return fetchmany(pages=pages, limit=1).pop()


def fetchall(pages) -> List[dict]:
    return fetchmany(pages=pages, limit=-1)


"""
Adapted from:
https://stackoverflow.com/questions/55546027/how-to-assign-arbitrary-metadata-to-pyarrow-table-parquet-columns
"""

def create_table_metadata(column_names, expected_rows, name, aliases):
    retval = {
        "_expected_rows": expected_rows,
        "_name": name,
        "_aliases": [a for a in set(aliases + [name]) if a],
    }
    for column in column_names:
        retval[column] = {"_aliases":[column]}
        for a in retval["_aliases"]:
            retval[column]['_aliases'].append(f"{a}.{column}")
    return retval

def set_metadata(tbl, tbl_meta=None):
    """
    Store table-level metadata as json-encoded byte strings.

    Table-level metadata is stored in the table's schema.

    Args:
        tbl (pyarrow.Table): The table to store metadata in
        tbl_meta: A json-serializable dictionary with table-level metadata.
    """
    import pyarrow as pa
    import orjson
    # Create updated column fields with new metadata
    if tbl_meta:

        fields = []
        for col in tbl.schema.names:
            fields.append(tbl.field(col))

        # Get updated table metadata
        tbl_metadata = tbl.schema.metadata or {}
        for k, v in tbl_meta.items():
            if isinstance(v, bytes):
                tbl_metadata[k] = v
            else:
                tbl_metadata[k] = orjson.dumps(v)

        # Create new schema with updated table metadata
        schema = pa.schema(fields, metadata=tbl_metadata)
        # With updated schema build new table (shouldn't copy data)
        tbl = tbl.cast(schema)

    return tbl

def _decode_metadata(metadata):
    """
    Arrow stores metadata keys and values as bytes. We store "arbitrary" data as
    json-encoded strings (utf-8), which are here decoded into normal dict.
    """
    import orjson

    if not metadata:
        # None or {} are not decoded
        return metadata

    decoded = {}
    for k, v in metadata.items():
        key = k.decode('utf-8')
        val = orjson.loads(v)
        decoded[key] = val
    return decoded


def get_metadata(tbl):
    """Get table metadata as dict."""
    return _decode_metadata(tbl.schema.metadata)
