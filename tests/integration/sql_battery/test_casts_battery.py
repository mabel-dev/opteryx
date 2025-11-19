"""
The best way to test a SQL Engine is to throw queries at it.

This suite focuses on testing the CAST operations.
"""
import os
import pytest
import sys

#import opteryx

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.utils.formatter import format_sql


# fmt:off
STATEMENTS = [

    # Is this dataset the shape we expect
    ("SELECT * FROM testdata.tweets", 100000, 14, None),

    # INTEGER
    ("SELECT 1 FROM testdata.tweets WHERE CAST(str_following AS INTEGER) = following", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST('-' || str_following AS INTEGER) = following * -1", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST(str_following AS INTEGER) AS VARCHAR) = str_following", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST(str_following AS INTEGER) AS BLOB) = str_following", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST('-' || str_following AS INTEGER) AS VARCHAR) = '-' || str_following", 99251, 1, None), # '-0' != '0'
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST('-' || str_following AS INTEGER) AS BLOB) = '-' || str_following", 99251, 1, None), # '-0' != '0'

    # DOUBLE
    ("SELECT 1 FROM testdata.tweets WHERE CAST(str_dbl_following AS DOUBLE) = (following / 10)", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST('-' || str_dbl_following AS DOUBLE) = (following / 10) * -1", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST(str_dbl_following AS DOUBLE) AS VARCHAR) = str_dbl_following", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST(str_dbl_following AS DOUBLE) AS BLOB) = str_dbl_following", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST('-' || str_dbl_following AS DOUBLE) AS VARCHAR) = '-' || str_dbl_following", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(CAST('-' || str_dbl_following AS DOUBLE) AS BLOB) = '-' || str_dbl_following", 100000, 1, None),

    # TIMESTAMP
    ("SELECT 1 FROM testdata.tweets WHERE CAST(str_timestamp AS TIMESTAMP) == ts_timestamp", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE str_timestamp || '.000000' == CAST(ts_timestamp AS VARCHAR)", 100000, 1, None),
    ("SELECT 1 FROM testdata.tweets WHERE CAST(int_timestamp * 1_000_000 AS TIMESTAMP) == ts_timestamp", 100000, 1, None),

    # Additional CAST edge cases - NULL handling
    ("SELECT CAST(NULL AS INTEGER)", 1, 1, None),
    ("SELECT CAST(NULL AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(NULL AS DOUBLE)", 1, 1, None),
    ("SELECT CAST(NULL AS BOOLEAN)", 1, 1, None),
    ("SELECT CAST(NULL AS TIMESTAMP)", 1, 1, None),

    # BOOLEAN casts
    ("SELECT CAST(1 AS BOOLEAN)", 1, 1, None),
    ("SELECT CAST(0 AS BOOLEAN)", 1, 1, None),
    ("SELECT CAST('true' AS BOOLEAN)", 1, 1, None),
    ("SELECT CAST('false' AS BOOLEAN)", 1, 1, None),
    ("SELECT CAST(TRUE AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(FALSE AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(TRUE AS INTEGER)", 1, 1, None),
    ("SELECT CAST(FALSE AS INTEGER)", 1, 1, None),

    # VARCHAR casts with special characters
    ("SELECT CAST('hello world' AS VARCHAR)", 1, 1, None),
    ("SELECT CAST('123' AS INTEGER)", 1, 1, None),
    ("SELECT CAST('123.456' AS DOUBLE)", 1, 1, None),
    ("SELECT CAST('2023-01-01' AS TIMESTAMP)", 1, 1, None),

    # Numeric edge cases
    ("SELECT CAST(0 AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(-0 AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(0.0 AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(-0.0 AS VARCHAR)", 1, 1, None),

    # Large numbers
    ("SELECT CAST(999999999999 AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(-999999999999 AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(1.7976931348623157e+308 AS VARCHAR)", 1, 1, None),

    # Scientific notation
    ("SELECT CAST('1e10' AS DOUBLE)", 1, 1, None),
    ("SELECT CAST('1.5e-5' AS DOUBLE)", 1, 1, None),
    ("SELECT CAST('-2.5e3' AS DOUBLE)", 1, 1, None),

    # Empty string casts
    ("SELECT CAST('' AS VARCHAR)", 1, 1, None),
    ("SELECT CAST('' AS BLOB)", 1, 1, None),

    # Special numeric values (these may need adjustment based on engine support)
    # ("SELECT CAST('inf' AS DOUBLE)", 1, 1, None),
    # ("SELECT CAST('-inf' AS DOUBLE)", 1, 1, None),
    # ("SELECT CAST('nan' AS DOUBLE)", 1, 1, None),

    # Cross-type casting chains
    ("SELECT CAST(CAST(CAST(123 AS VARCHAR) AS INTEGER) AS DOUBLE)", 1, 1, None),
    ("SELECT CAST(CAST(CAST('456' AS INTEGER) AS DOUBLE) AS VARCHAR)", 1, 1, None),
    ("SELECT CAST(CAST(TRUE AS INTEGER) AS VARCHAR)", 1, 1, None),

    # VARBINARY casts
    ("SELECT CAST('test' AS BLOB)", 1, 1, None),
    ("SELECT CAST(CAST('test' AS BLOB) AS VARCHAR)", 1, 1, None),
    ("SELECT CAST('test' AS VARBINARY)", 1, 1, None),
    ("SELECT VARBINARY('test')", 1, 1, None),
    ("SELECT TRY_VARBINARY('test')", 1, 1, None),
    ("SELECT 'test'::VARBINARY", 1, 1, None),
    ("SELECT CAST(CAST('test' AS VARBINARY) AS VARCHAR)", 1, 1, None),

]
# fmt:on


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement:str, rows:int, columns:int, exception: Optional[Exception]):
    """
    Test an battery of statements
    """

    try:
        # query to arrow is the fastest way to query
        result = opteryx.query_to_arrow(statement, memberships=["Apollo 11", "opteryx"])
        actual_rows, actual_columns = result.shape
        assert (
            rows == actual_rows
        ), f"\n\033[38;5;203mQuery returned {actual_rows} rows but {rows} were expected.\033[0m\n{statement}"
        assert (
            columns == actual_columns
        ), f"\n\033[38;5;203mQuery returned {actual_columns} cols but {columns} were expected.\033[0m\n{statement}"
        assert (
            exception is None
        ), f"Exception {exception} not raised but expected\n{format_sql(statement)}"
    except AssertionError as error:
        raise error
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error


if __name__ == "__main__":  # pragma: no cover
    # Running in the IDE we do some formatting - it's not functional but helps when reading the outputs.

    import shutil
    import time

    from tests import trunc_printable

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed:int = 0
    failed:int = 0
    nl:str = "\n"
    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} CAST TESTS")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        printable = statement
        if hasattr(printable, "decode"):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_sql_battery(statement, rows, cols, err)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(f" \033[0;31m{failed}\033[0m")
            else:
                print()
        except Exception as err:
            failed += 1
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ {failed}\033[0m")
            print(">", err)
            failures.append((statement, err))
            
            #print(opteryx.query(statement))
            #raise err

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
