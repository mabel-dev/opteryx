"""
The best way to test a SQL Engine is to throw queries at it.

This is testing the SQL connector.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
# fmt:off
STATEMENTS = [
        ("SELECT * FROM opteryx.satellites", 177, 8, None),
        ("SELECT * FROM opteryx.planets", 9, 20, None),
        ("SELECT * FROM opteryx.astronauts", 357, 19, None),
        ("SELECT * FROM opteryx.missions", 4630, 8, None),
        ("SELECT * FROM opteryx.nvd", 256979, 16, None),
]


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_tarchia_battery(statement, rows, columns, exception):
    """
    Test an battery of statements
    """
    try:
        result = opteryx.query_to_arrow(statement)
        actual_rows, actual_columns = result.shape

        assert (
            rows == actual_rows
        ), f"Query returned {actual_rows} rows but {rows} were expected"
        f" ({actual_columns} vs {columns})\n{statement}"
        assert (
            columns == actual_columns
        ), f"Query returned {actual_columns} cols but {columns} were"
        f" expected\n{statement}"
    except AssertionError as err:
        raise Exception(err) from err
    except Exception as err:
        if type(err) != exception:
            raise Exception(
                f"{format_sql(statement)}\nQuery failed with error {type(err)} but error {exception} was expected"
            ) from err


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    from opteryx.utils.formatter import format_sql
    from tests.tools import trunc_printable

    start_suite = time.monotonic_ns()

    width = shutil.get_terminal_size((80, 20))[0] - 15

    passed = 0
    failed = 0

    nl = "\n"

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TARCHIA TESTS")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        start = time.monotonic_ns()
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
            test_tarchia_battery(statement, rows, cols, err)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ *\033[0m")
            print(">", err)
            failed += 1

    print("--- ✅ \033[0;32mdone\033[0m")
    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
