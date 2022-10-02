"""
Test that the queries used in documentation execute without error
"""
import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def get_tests(test_type):
    import glob

    suites = glob.glob(f"**/**.{test_type}", recursive=True)
    for suite in suites:
        with open(suite, mode="r") as test_file:
            yield from [
                line
                for line in test_file.read().splitlines()
                if len(line) > 0 and line[0] != "#"
            ]


RUN_ONLY_TESTS = list(get_tests("run_tests"))


@pytest.mark.parametrize("statement", RUN_ONLY_TESTS)
def test_run_only_tests(statement):
    """
    These tests are only run, the result is not checked.
    This is useful for parsing checks
    """
    conn = opteryx.connect()
    cursor = conn.cursor()

    cursor.execute(statement)
    cursor.arrow()


if __name__ == "__main__":  # pragma: no cover

    print(f"RUNNING BATTERY OF {len(RUN_ONLY_TESTS)} RUN_ONLY TESTS")
    for statement in RUN_ONLY_TESTS:
        print(statement)
        test_run_only_tests(statement)

    print("✅ okay")
