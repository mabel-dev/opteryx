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

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import orjson

import opteryx


def get_tests(test_type):
    suites = glob.glob(f"**/**.{test_type}", recursive=True)
    for suite in suites:
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
    conn = opteryx.connect()
    cursor = conn.cursor()

    sql = test["statement"]

    cursor.execute(sql)
    result = cursor.arrow().to_pydict()

    printable_result = orjson.dumps(result).decode()
    printable_expected = orjson.dumps(test["result"]).decode()

    assert (
        result == test["result"]
    ), f"Outcome:\n{printable_result}\nExpected:\n{printable_expected}"


if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time

    width = shutil.get_terminal_size((80, 20))[0] - 40

    print(f"RUNNING BATTERY OF {len(RESULTS_TESTS)} RESULTS TESTS")
    for index, test in enumerate(RESULTS_TESTS):
        start = time.monotonic_ns()
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {test['statement'][0:width - 1].ljust(width)}",
            "\033[0;35m" + test["file"].split("/")[-1].split(".")[0][0:25].ljust(25) + "\033[0m",
            end="",
        )

        test_results_tests(test)

        print(f"\033[0;32m{str(int((time.monotonic_ns() - start)/1000000)).rjust(4)}ms\033[0m ✅")

    print("--- ✅ \033[0;32mdone\033[0m")
