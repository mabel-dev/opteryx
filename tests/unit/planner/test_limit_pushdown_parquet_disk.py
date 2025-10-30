import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from tests import is_arm, is_mac, is_windows, skip_if


STATEMENTS = [
    # baseline
    ("SELECT name FROM testdata.planets;", 9),
    # push limit
    ("SELECT name FROM testdata.planets LIMIT 1;", 1),
    # test with filter
    ("SELECT name FROM testdata.planets WHERE gravity > 1;", 8),
    # pushable filter and limit should push the limit
    ("SELECT name FROM testdata.planets WHERE gravity > 1 LIMIT 1;", 1),
    # if we can't push the filter, we can't push the limit
    ("SELECT name FROM testdata.planets WHERE SIGNUM(gravity) > 1 LIMIT 1;", 9),
    # we don't push past ORDER BY
    ("SELECT * FROM testdata.planets ORDER BY name LIMIT 3", 9),
    # push past subqueries
    ("SELECT name FROM (SELECT * FROM testdata.planets) AS S LIMIT 3", 3),
    ("SELECT name FROM testdata.planets;", 9),
]

@skip_if(is_arm() or is_windows() or is_mac())
@pytest.mark.parametrize("query, expected_rows", STATEMENTS)
def test_parquet_disk_limit_pushdown(query, expected_rows):

    cur = opteryx.query(query)
    cur.materialize()
    assert cur.stats["rows_read"] == expected_rows, cur.stats


@skip_if(is_arm() or is_windows() or is_mac())
def test_limit_pushdown_projection_plan():
    query = "SELECT name FROM (SELECT name FROM testdata.planets) AS s LIMIT 3;"
    cur = opteryx.query(query)
    cur.materialize()
    plan_lines = cur.stats["executed_plan"].splitlines()
    scan_line = next(line for line in plan_lines if "READ (testdata.planets)" in line)
    assert "LIMIT 3" in scan_line, cur.stats["executed_plan"]
    assert cur.stats["rows_read"] == 3, cur.stats


@skip_if(is_arm() or is_windows() or is_mac())
def test_limit_not_pushed_past_heap_sort():
    query = "SELECT name FROM testdata.planets ORDER BY name LIMIT 3;"
    cur = opteryx.query(query)
    cur.materialize()
    plan_lines = cur.stats["executed_plan"].splitlines()
    heap_sort_line = next(line for line in plan_lines if "HEAP SORT" in line)
    scan_line = next(line for line in plan_lines if "READ (testdata.planets)" in line)
    assert "LIMIT" in heap_sort_line  # fused limit stays with heap sort
    assert "LIMIT" not in scan_line, cur.stats["executed_plan"]
    assert cur.stats["rows_read"] == 9, cur.stats

if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time

    from tests import trunc_printable
    from opteryx.utils.formatter import format_sql

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TESTS")
    for index, (statement, read_columns) in enumerate(STATEMENTS):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_parquet_disk_limit_pushdown(statement, read_columns)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except (AssertionError, opteryx.exceptions.Error) as err:
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
