"""
The best way to test a SQL Engine is to throw queries at it.

We use a 1GB Raspberry Pi for ARM tests. We have come tests that are pusrposefully
large to test what happens at those limits. Exclude those from the ARM test suite.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

from opteryx.connectors import DiskConnector
from tests.tools import skip_on_partials

# fmt:off
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM $satellites", 177, 8, None),
        ("SELECT * FROM $planets", 9, 20, None),
        ("SELECT * FROM $astronauts", 357, 19, None),

        # Large results can't be added to pages [#453]
        ("SELECT SHA512(column_0) FROM FAKE(150000, 1)", 150000, 1, None),
        # Low cardinality INNER JOINS blow memory [#444]
        ("SELECT COUNT(*) FROM (SELECT * FROM testdata.formats.parquet WITH(NO_PARTITION) LIMIT 50) INNER JOIN testdata.formats.parquet WITH(NO_PARTITION) USING (user_verified)", 1, 1, None),
]


@skip_on_partials
@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement, rows, columns, exception):
    """
    Test an battery of statements
    """

    opteryx.register_store("tests", DiskConnector)

    conn = opteryx.connect()
    cursor = conn.cursor()
    try:
        cursor.execute(statement)
        actual_rows, actual_columns = cursor.shape

        assert (
            rows == actual_rows
        ), f"Query returned {actual_rows} rows but {rows} were expected"
        f" ({actual_columns} vs {columns})\n{statement}\n{cursor.head(10)}"
        assert (
            columns == actual_columns
        ), f"Query returned {actual_columns} cols but {columns} were"
        f" expected\n{statement}\n{cursor.head(10)}"
    except Exception as err:
        assert type(err) == exception, f"Query failed with error {type(err)}"
        f" but error {exception} was expected"


if __name__ == "__main__":  # pragma: no cover

    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    width = shutil.get_terminal_size((80, 20))[0] - 15

    nl = "\n"

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SHAPE TESTS (NOT ARM)")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        start = time.monotonic_ns()
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {statement[0:width - 1].ljust(width)}",
            end="",
        )
        test_sql_battery(statement, rows, cols, err)
        print(
            f"\033[0;32m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅"
        )

    print("--- ✅ \033[0;32mdone\033[0m")
