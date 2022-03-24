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

## get_column_name(alias)
##  read the column metadata
##  return matching columns



def create_table_metadata(table, expected_rows, name, table_aliases):
    
    # we're going to replace the column names with random strings
    def random_string(length: int = 32) -> str:
        import os
        import base64
        # we're creating a series of random bytes, 3/4 the length
        # of the string we want, base64 encoding it (which makes 
        # it longer) and then returning the length of string
        # requested.
        b = os.urandom(-((length * -3)//4))
        return base64.b64encode(b).decode("utf8")[:length]


    if not isinstance(table_aliases, list):
        table_aliases = [table_aliases]

    # create the table information we're going to track
    table_metadata = {
        "expected_rows": expected_rows,
        "name": name,
        "aliases": [a for a in set(table_aliases + [name]) if a]
    }

    # column information includes all the aliases a column is known by
    column_metadata = {}
    for column in table.column_names:
        # we're going to rename the columns
        new_column = random_string(32)
        # the column is know aliased by it's previous name 
        column_metadata[new_column] = {"aliases": [column]}
        # the column prefers it's current name
        column_metadata[new_column]["preferred_name"] = column
        # for every alias the table has, the column is also know by that
        for a in table_metadata["aliases"]:
            column_metadata[new_column]["aliases"].append(f"{a}.{column}")
    
    # rename the columns
    table = table.rename_columns(list(column_metadata.keys()))
    # add the metadata
    return set_metadata(table, table_metadata=table_metadata, column_metadata=column_metadata)

def get_preferred_column_names(table):
    metadata = column_metadata(table)
    return [(c, v.get("preferred_name", None)) for c, v in metadata.items()]

def get_column_from_alias(table, column):
    """
    For a given alias, return all of the matching columns (usually one)
    """
    matches = []
    metadata = column_metadata(table)
    for k,v in metadata.items():
        matches.extend([k for alias in v.get("aliases", []) if alias == column])
    return matches

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
    import orjson

    # Create updated column fields with new metadata
    if table_metadata or column_metadata:

        fields = []
        for name in table.schema.names:
            col = table.field(name)
            if col.name in column_metadata:
                # Get updated column metadata
                metadata = col.metadata or {}
                for k, v in column_metadata[name].items():
                    metadata[k] = orjson.dumps(v)
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
                    tbl_metadata[k] = orjson.dumps(v)

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
    import orjson

    if not metadata:
        # None or {} are not decoded
        return metadata

    decoded = {}
    for k, v in metadata.items():
        key = k.decode("utf-8")
        val = orjson.loads(v)
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


if __name__ == "__main__":

    import os
    import sys

    sys.path.insert(1, os.path.join(sys.path[0], "../../"))

    from opteryx.samples import planets

    p = planets()
    p = create_table_metadata(p, 9, "planets", "p")

    print(get_preferred_column_names(p))
    print(get_column_from_alias(p, "p.name"))
    print(get_column_from_alias(p, "name"))
    print(get_column_from_alias(p, "planets.name"))