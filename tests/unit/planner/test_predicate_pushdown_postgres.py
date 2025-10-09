"""
Test predicate pushdown using the sql connector
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import time

import pytest

import opteryx
from opteryx.connectors import SqlConnector
from opteryx.utils.formatter import format_sql
from tests.tools import is_arm, is_mac, is_version, is_windows, skip_if

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_USER = os.environ.get("POSTGRES_USER")


opteryx.register_store(
    "pg",
    SqlConnector,
    remove_prefix=True,
    connection=f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@trumpet.db.elephantsql.com/{POSTGRES_USER}",
)

test_cases = [
    ("SELECT * FROM pg.planets WHERE gravity <= 3.7", 3, 3),
    ("SELECT * FROM pg.planets WHERE name != 'Earth'", 8, 8),
    ("SELECT * FROM pg.planets WHERE name != 'E\"arth'", 9, 9),
    ("SELECT * FROM pg.planets WHERE gravity != 3.7", 7, 7),
    ("SELECT * FROM pg.planets WHERE gravity < 3.7", 1, 1),
    ("SELECT * FROM pg.planets WHERE gravity > 3.7", 6, 6),
    ("SELECT * FROM pg.planets WHERE gravity >= 3.7", 8, 8),
    ("SELECT * FROM pg.planets WHERE name LIKE '%a%'", 4, 4),
    ("SELECT * FROM pg.planets WHERE id > gravity", 2, 9),
    ("SELECT * FROM pg.planets WHERE surface_pressure IS NULL", 4, 4),
    ("SELECT * FROM pg.planets WHERE gravity - mass > 10", 0, 9),
]


# skip to reduce contention
@skip_if(is_arm() or is_windows() or is_mac() or not is_version("3.11"))
@pytest.mark.parametrize("statement,expected_rowcount,expected_rows_read", test_cases)
def test_predicate_pushdown_postgres_parameterized(
    statement, expected_rowcount, expected_rows_read
):
    res = opteryx.query(statement)
    assert res.rowcount == expected_rowcount, f"Expected {expected_rowcount}, got {res.rowcount}"
    assert (
        res.stats.get("rows_read", 0) == expected_rows_read
    ), f"Expected {expected_rows_read}, got {res.stats.get('rows_read', 0)}"


if __name__ == "__main__":  # pragma: no cover
    import shutil

    from tests.tools import trunc_printable

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} TESTS")
    for index, (statement, returned_rows, read_rows) in enumerate(test_cases):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_predicate_pushdown_postgres_parameterized(statement, returned_rows, read_rows)
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

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
