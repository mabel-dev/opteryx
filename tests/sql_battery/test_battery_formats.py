"""
The best way to test a SQL engine is to throw queries at it.

This tests the various format readers.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest

import opteryx
from tests.tools import is_pypy, is_windows

# fmt:off
STATEMENTS = [
        # Two tests, one to test the file can be opened and read, one to test the
        # values that have been read.

        # arrow (feather)
        ("SELECT * FROM 'testdata/flat/formats/arrow'", 100000, 13, False),
        ("SELECT user_name, user_verified FROM 'testdata/flat/formats/arrow' WHERE user_name ILIKE '%news%'", 122, 2, False),

        # avro
        ("SELECT * FROM testdata.flat.formats.avro", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.avro WHERE user_name ILIKE '%news%'", 122, 2, False),

        # jsonl
        ("SELECT * FROM testdata.flat.formats.jsonl", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.jsonl WHERE user_name ILIKE '%news%'", 122, 2, False),

        # orc
        ("SELECT * FROM testdata.flat.formats.orc", 100000, 13, is_windows() or is_pypy()),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.orc WHERE user_name ILIKE '%news%'", 122, 2, is_windows() or is_pypy()),

        # parquet
        ("SELECT * FROM testdata.flat.formats.parquet", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WHERE user_name ILIKE '%news%'", 122, 2, False),

        # zstandard jsonl
        ("SELECT * FROM testdata.flat.formats.zstd", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.zstd WHERE user_name ILIKE '%news%'", 122, 2, False),

        # csv - has a different input file
        ("SELECT * FROM testdata.flat.formats.csv", 33529, 10, False),
        ("SELECT username, user_verified FROM testdata.flat.formats.csv WHERE username ILIKE '%cve%'", 2532, 2, False),

        # tsv - has the same file as csv
        ("SELECT * FROM testdata.flat.formats.tsv", 33529, 10, False),
        ("SELECT username, user_verified FROM testdata.flat.formats.tsv WHERE username ILIKE '%cve%'", 2532, 2, False),

        # .json.parquet - appears to be handled incorrectly
        ("SELECT * FROM testdata.flat.formats.misnamed_parquet", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.misnamed_parquet WHERE user_name ILIKE '%news%'", 122, 2, False),

        # PyArrow IPC streams
        ("SELECT * FROM testdata.flat.formats.ipc", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.ipc WHERE user_name ILIKE '%news%'", 122, 2, False),

        # PyArrow IPC streams
        ("SELECT * FROM testdata.flat.formats.ipc_lz4", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.ipc WHERE user_name ILIKE '%news%'", 122, 2, False),
        
        # PyArrow IPC streams
        ("SELECT * FROM testdata.flat.formats.ipc_zstd", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.ipc WHERE user_name ILIKE '%news%'", 122, 2, False),
        
        # Mabel legacy LZMA compressed JSONL format (different input file)
        ("SELECT * FROM testdata.flat.formats.lzma", 71981, 14, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.lzma WHERE user_name ILIKE '%news%'", 112, 2, False),

        # Pipe (|) Separated Values (different input file)
        ("SELECT * FROM testdata.flat.formats.psv", 586, 16, False),
        ("SELECT L_SHIPINSTRUCT, L_LINESTATUS FROM testdata.flat.formats.psv WHERE L_SHIPMODE ILIKE '%O%'", 90, 2, False),
    ]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns, skip", STATEMENTS)
def test_sql_battery(statement, rows, columns, skip):
    """
    Test an battery of statements
    """
    if skip:  # pragma: no cover
        print(f"Skipping testcase on unsupported platform - {statement}")
        return

    #    opteryx.register_store("tests", DiskConnector)

    result = opteryx.query_to_arrow(statement)
    actual_rows, actual_columns = result.shape

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} FORMAT TESTS")
    for statement, rows, cols, skip in STATEMENTS:
        print(statement)
        test_sql_battery(statement, rows, cols, skip)

    print("✅ okay")
