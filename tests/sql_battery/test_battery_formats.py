"""
The best way to test a SQL engine is to throw queries at it.

This tests the various format readers.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest
from tests.tools import is_pypy, is_windows

import opteryx

# fmt:off
STATEMENTS = [
        # Two tests, one to test the file can be opened and read, one to test the
        # values that have been read.

        # arrow (feather)
        ("SELECT * FROM 'testdata/flat/formats/arrow' WITH (NO_PARTITION)", 100000, 13, False),
        ("SELECT user_name, user_verified FROM 'testdata/flat/formats/arrow' WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2, False),

        # avro
        ("SELECT * FROM testdata.flat.formats.avro WITH (NO_PARTITION)", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.avro WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2, False),

        # jsonl
        ("SELECT * FROM testdata.flat.formats.jsonl WITH (NO_PARTITION)", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.jsonl WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2, False),

        # orc
        ("SELECT * FROM testdata.flat.formats.orc WITH (NO_PARTITION)", 100000, 13, is_windows() or is_pypy()),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.orc WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2, is_windows() or is_pypy()),

        # parquet
        ("SELECT * FROM testdata.flat.formats.parquet WITH (NO_PARTITION)", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2, False),

        # zstandard jsonl
        ("SELECT * FROM testdata.flat.formats.zstd WITH (NO_PARTITION)", 100000, 13, False),
        ("SELECT user_name, user_verified FROM testdata.flat.formats.zstd WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2, False),

        # csv - has a different input file
        ("SELECT * FROM testdata.flat.formats.csv WITH (NO_PARTITION)", 33529, 10, False),
        ("SELECT username, user_verified FROM testdata.flat.formats.csv WITH(NO_PARTITION) WHERE username ILIKE '%cve%'", 2532, 2, False),

        # tsv - has the same file as csv
        ("SELECT * FROM testdata.flat.formats.tsv WITH (NO_PARTITION)", 33529, 10, False),
        ("SELECT username, user_verified FROM testdata.flat.formats.tsv WITH(NO_PARTITION) WHERE username ILIKE '%cve%'", 2532, 2, False),
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

    conn = opteryx.connect()
    cursor = conn.cursor()
    cursor.execute(statement)
    actual_rows, actual_columns = cursor.shape

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}\n{cursor.head(10)}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}\n{cursor.head(10)}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} FORMAT TESTS")
    for statement, rows, cols, skip in STATEMENTS:
        print(statement)
        test_sql_battery(statement, rows, cols, skip)

    print("✅ okay")
