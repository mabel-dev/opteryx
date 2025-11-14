import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.utils.formatter import format_sql

# fmt: off
STATEMENTS = [
    ("SELECT * FROM testdata.planets", 9, 20, None),
    ("SELECT * FROM testdata.satellites", 177, 8, None),
    ("SELECT COUNT(*) FROM testdata.planets;", 1, 1, None),
    ("SELECT COUNT(*) FROM testdata.satellites;", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT * FROM testdata.planets) AS p", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT COUNT(*) FROM testdata.planets) AS p", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT * FROM testdata.planets WHERE id > 4) AS p", 1, 1, None),
    ("SELECT COUNT(*) FROM (SELECT * FROM testdata.planets) AS p WHERE id > 4", 1, 1, None),
    ("SELECT name FROM testdata.planets;", 9, 1, None),
    ("SELECT name FROM testdata.satellites;", 177, 1, None),
    ("SELECT * FROM testdata.planets INNER JOIN $satellites ON testdata.planets.id = $satellites.planetId;", 177, 28, None),
    ("SELECT * FROM testdata.planets, $satellites WHERE testdata.planets.id = $satellites.planetId;", 177, 28, None),
    ("SELECT * FROM testdata.planets CROSS JOIN $satellites WHERE testdata.planets.id = $satellites.planetId;", 177, 28, None),
    ("SELECT * FROM testdata.planets INNER JOIN testdata.satellites ON testdata.planets.id = testdata.satellites.planetId;", 177, 28, None),
    ("SELECT name FROM testdata.planets WHERE name LIKE 'Earth';", 1, 1, None),
    ("SELECT * FROM testdata.planets WHERE id > gravity", 2, 20, None),
    ("SELECT * FROM testdata.planets WHERE surfacePressure IS NULL", 4, 20, None),
    ("SELECT * FROM testdata.planets WHERE surfacePressure IS NOT NULL", 5, 20, None),
    ("SELECT * FROM testdata.planets, testdata.satellites WHERE testdata.planets.id = 5 AND testdata.satellites.planetId = 5;", 67, 28, None),
    ("SELECT * FROM testdata.planets, testdata.satellites WHERE testdata.planets.id - testdata.satellites.planetId = 0;", 177, 28, None),
    ("SELECT * FROM testdata.planets, testdata.satellites WHERE testdata.planets.id - testdata.satellites.planetId != 0;", 1416, 28, None),
    ("SELECT * FROM testdata.planets WHERE testdata.planets.id - testdata.planets.numberOfMoons < 0;", 4, 20, None),
]
# fmt: on


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement, rows, columns, exception):
    """
    Test an battery of statements
    """
    try:
        # query to arrow is the fastest way to query
        result = opteryx.query_to_arrow(statement)
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
    except AssertionError as err:  # pragma: no cover
        raise Exception(err) from err
    except Exception as err:  # pragma: no cover
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

    from tests import trunc_printable

    start_suite = time.monotonic_ns()

    width = shutil.get_terminal_size((80, 20))[0] - 15

    passed = 0
    failed = 0

    nl = "\n"

    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} testdata TESTS")
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
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ *\033[0m")
            print(">", err)
            failed += 1
            failures.append((statement, err))

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
