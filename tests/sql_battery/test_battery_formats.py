"""
The best way to test a SQL engine is to throw queries at it.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pyarrow
import pytest

import opteryx
from opteryx.storage.adapters import DiskStorage
from opteryx.utils.arrow import fetchmany
from opteryx.utils.display import ascii_table

# fmt:off
STATEMENTS = [
        # arrow (feather)
#        ("SELECT * FROM tests.data.formats.arrow", 100000, 13),

        # avro
#        ("SELECT * FROM tests.data.formats.avro", 100000, 13),

        # jsonl
        ("SELECT * FROM tests.data.formats.jsonl", 100000, 13),

        # orc
#        ("SELECT * FROM tests.data.formats.orc", 100000, 13),

        # parquet
#        ("SELECT * FROM tests.data.formats.parquet", 100000, 13),

        # zstandard jsonl
#        ("SELECT * FROM tests.data.formats.zstd", 100000, 13),
    ]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns", STATEMENTS)
def test_sql_battery(statement, rows, columns):
    """
    Test an battery of statements
    """
    conn = opteryx.connect(reader=DiskStorage(), partition_scheme=None)
    cursor = conn.cursor()
    cursor.execute(statement)

    cursor._results = list(cursor._results)
    if cursor._results:
        result = pyarrow.concat_tables(cursor._results)
        actual_rows, actual_columns = result.shape
    else:
        result = None
        actual_rows, actual_columns = 0, 0

    assert (
        rows == actual_rows
    ), f"Query returned {actual_rows} rows but {rows} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10))}"
    assert (
        columns == actual_columns
    ), f"Query returned {actual_columns} cols but {columns} were expected, {statement}\n{ascii_table(fetchmany(result, limit=10))}"


if __name__ == "__main__":

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} FORMAT TESTS")
    for statement, rows, cols in STATEMENTS:
        print(statement)
        test_sql_battery(statement, rows, cols)
    print("okay")
