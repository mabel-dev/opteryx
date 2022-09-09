"""
The best way to test a SQL engine is to throw queries at it.

This tests the various format readers.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pyarrow
import pytest

import opteryx
from opteryx.connectors import DiskConnector
from opteryx.utils.arrow import fetchmany
from opteryx.utils.display import ascii_table

# fmt:off
STATEMENTS = [
        # Two tests, one to test the file can be opened and read, one to test the
        # values that have been read.

        # arrow (feather)
        ("SELECT * FROM testdata.formats.arrow WITH(NO_PARTITION)", 100000, 13),
        ("SELECT user_name, user_verified FROM testdata.formats.arrow WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2),

        # jsonl
        ("SELECT * FROM testdata.formats.jsonl WITH(NO_PARTITION)", 100000, 13),
        ("SELECT user_name, user_verified FROM testdata.formats.jsonl WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2),

        # orc
        ("SELECT * FROM testdata.formats.orc WITH(NO_PARTITION)", 100000, 13),
        ("SELECT user_name, user_verified FROM testdata.formats.orc WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2),

        # parquet
        ("SELECT * FROM testdata.formats.parquet WITH(NO_PARTITION)", 100000, 13),
        ("SELECT user_name, user_verified FROM testdata.formats.parquet WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2),

        # zstandard jsonl
        ("SELECT * FROM testdata.formats.zstd WITH(NO_PARTITION)", 100000, 13),
        ("SELECT user_name, user_verified FROM testdata.formats.zstd WITH(NO_PARTITION) WHERE user_name ILIKE '%news%'", 122, 2),

        # csv - has a different input file
        ("SELECT * FROM testdata.formats.csv WITH(NO_PARTITION)", 29751, 10),
        ("SELECT username, user_verified FROM testdata.formats.csv WITH(NO_PARTITION) WHERE username ILIKE '%cve%'", 2002, 2),
    ]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns", STATEMENTS)
def test_sql_battery(statement, rows, columns):
    """
    Test an battery of statements
    """
    opteryx.register_store("tests", DiskConnector)

    conn = opteryx.connect()
    cursor = conn.cursor()
    cursor.execute(statement)

    cursor._results = list(cursor._results)
    if cursor._results:
        result = pyarrow.concat_tables(cursor._results, promote=True)
        actual_rows, actual_columns = result.shape
    else:  # pragma: no cover
        result = None
        actual_rows, actual_columns = 0, 0

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10))}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10))}"


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} FORMAT TESTS")
    for statement, rows, cols in STATEMENTS:
        print(statement)
        test_sql_battery(statement, rows, cols)

    print("✅ okay")
