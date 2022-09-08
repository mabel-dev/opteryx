"""
The best way to test a SQL Engine is to throw queries at it.

This is a set of tests which aren't fast to run so aren't run as part of the main set.

These are almost always going to be volume-related issues.

These will still be run by the GitHub Actions, but not when running the main set
locally.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow
import pytest

import opteryx

from opteryx.utils.arrow import fetchmany
from opteryx.utils.display import ascii_table
from opteryx.connectors import DiskConnector


# fmt:off
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM $satellites", 177, 8),
        ("SELECT * FROM $planets", 9, 20),
        ("SELECT * FROM $astronauts", 357, 19),

        # Large results can't be added to pages [#453]
        ("SELECT SHA512(column_0) FROM FAKE(150000, 1)", 150000, 1),
        # Low cardinality INNER JOINS blow memory [#444]
        ("SELECT COUNT(*) FROM (SELECT * FROM testdata.formats.parquet WITH(NO_PARTITION) LIMIT 50) INNER JOIN testdata.formats.parquet WITH(NO_PARTITION) USING (user_verified)", 1, 1)
    ]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns", STATEMENTS)
def test_sql_battery_slow_tests(statement, rows, columns):
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
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10), limit=10)}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10), limit=10)}"


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SHAPE TESTS")
    for index, (statement, rows, cols) in enumerate(STATEMENTS):
        print(f"{(index + 1):04}", statement)
        test_sql_battery_slow_tests(statement, rows, cols)

    print("✅ okay")
