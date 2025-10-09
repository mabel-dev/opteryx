"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
    Shape Checking
 >  Results Checking
    Compare to DuckDB

This is higher value than the other CI-executed SQL tests, the others being run-only
and shape-checking, this is also the most time consuming to write and maintain.

This suite executes a statement and confirms the output matches what was expected.
"""

import glob
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import orjson

import opteryx
from opteryx.utils.formatter import format_sql

OS_SEP = os.sep


def get_tests(test_type):
    suites = glob.glob(f"**/**.{test_type}", recursive=True)
    for suite in sorted(suites):
        with open(suite, mode="r") as test_file:
            try:
                yield {"file": suite, **orjson.loads(test_file.read())}
            except Exception as err:  # pragma: no cover
                print(err)
                print(suite)


RESULTS_TESTS = list(get_tests("results_tests"))


@pytest.mark.parametrize("test", RESULTS_TESTS)
def test_results_tests(test):
    """ """
    sql = test["statement"]
    result = opteryx.query_to_arrow(sql).to_pydict()

    printable_result = orjson.dumps(result, default=str, option=orjson.OPT_SORT_KEYS).decode()
    printable_expected = orjson.dumps(test["result"], option=orjson.OPT_SORT_KEYS).decode()

    assert (
        printable_result == printable_expected
    ), f"Outcome:\n{printable_result}\nExpected:\n{printable_expected}"


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    from tests import trunc_printable

    start_suite = time.monotonic_ns()

    width = shutil.get_terminal_size((80, 20))[0] - 42

    passed = 0
    failed = 0

    nl = "\n"

    failures = []

    print(f"RUNNING BATTERY OF {len(RESULTS_TESTS)} RESULTS TESTS")
    for index, test in enumerate(RESULTS_TESTS):
        printable = test["statement"]
        test_id = test["file"].split(OS_SEP)[-1].split(".")[0][0:25].ljust(25)
        if hasattr(printable, "decode"):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m",
            f"\033[0;35m{test_id}\033[0m",
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_results_tests(test)
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
            failures.append((test_id, test["statement"], err))

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for test, statement, err in failures:
            print(
                f"\033[38;2;26;185;67m{test}\033[0m\n{format_sql(statement)}\n\033[38;2;255;121;198m{err}\033[0m\n"
            )

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
