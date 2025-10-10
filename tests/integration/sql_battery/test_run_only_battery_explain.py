"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

 >  Run Only
    Shape Checking
    Results Checking
    Compare to DuckDB

This tests that the EXPLAIN prefixes don't cause execution errors - rather
than think of new queries to try this against, we use the same as the run
only tests, but with EXPLAIN in front.

Each query is run three times, once with EXPLAIN, once with EXPLAIN ANALYZE
and once with EXPLAIN ANALYZE FORMAT MERMAID.
"""

import glob
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def get_tests(test_type):
    suites = glob.glob(f"**/**.{test_type}", recursive=True)
    for suite in suites:
        with open(suite, mode="r") as test_file:
            yield from [
                line for line in test_file.read().splitlines() if len(line) > 0 and not line.startswith(("#", "--"))
            ]


RUN_ONLY_TESTS = list(get_tests("run_tests"))


@pytest.mark.parametrize("statement", RUN_ONLY_TESTS)
def test_run_only_tests_explain(statement):
    """
    These tests are only run, the result is not checked.
    This is useful for parsing checks
    """
    if statement.startswith("SELECT"):
        opteryx.query_to_arrow("EXPLAIN " + statement)

@pytest.mark.parametrize("statement", RUN_ONLY_TESTS)
def test_run_only_tests_explain_analyze(statement):
    """
    These tests are only run, the result is not checked.
    This is useful for parsing checks
    """
    if statement.startswith("SELECT"):
        opteryx.query_to_arrow("EXPLAIN ANALYZE " + statement)

@pytest.mark.parametrize("statement", RUN_ONLY_TESTS)
def test_run_only_tests_explain_analyze_format(statement):
    """
    These tests are only run, the result is not checked.
    This is useful for parsing checks
    """
    if statement.startswith("SELECT"):
        opteryx.query_to_arrow("EXPLAIN ANALYZE FORMAT MERMAID " + statement)

if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time

    from opteryx.utils.formatter import format_sql
    from tests import trunc_printable

    width = shutil.get_terminal_size((80, 20))[0] - 15

    nl = "\n"

    batch_start = time.monotonic_ns()
    print(f"RUNNING BATTERY OF {len(RUN_ONLY_TESTS)} RUN_ONLY EXPLAIN TESTS")
    for index, statement in enumerate(RUN_ONLY_TESTS):
        start = time.monotonic_ns()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )

        try:
            test_run_only_tests_explain(statement)
            test_run_only_tests_explain_analyze(statement)
            test_run_only_tests_explain_analyze_format(statement)
        except Exception as e:
            print(statement)
            raise e

        print(f"\033[0;32m{str(int((time.monotonic_ns() - start)/1000000)).rjust(4)}ms\033[0m ✅")

    print("--- ✅ \033[0;32mdone\033[0m")
    print(f"Total time: {str(int((time.monotonic_ns() - batch_start)/1000000)).rjust(4)}ms")
