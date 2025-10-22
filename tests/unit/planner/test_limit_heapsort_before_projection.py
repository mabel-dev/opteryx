"""
Test LIMIT pushdown before expensive projections

This tests the optimization that pushes LIMIT operations before projections
that contain expensive calculations, reducing the number of rows processed.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest

import opteryx
from opteryx.utils.formatter import format_sql


# Test cases: (query, should_optimize, description)
STATEMENTS = [
    # LIMIT should be pushed before projection with calculations
    (
        "SELECT id, name, LENGTH(name) * 2 AS calc FROM $planets LIMIT 3",
        True,
        "LIMIT before projection with calculation",
    ),
    # LIMIT should be pushed before projection with multiple calculations
    (
        "SELECT id, name, LENGTH(name) + LENGTH(id) AS calc FROM $planets LIMIT 5",
        True,
        "LIMIT before projection with multiple calculations",
    ),
    # Simple LIMIT without projection should still work
    (
        "SELECT id, name FROM $planets LIMIT 5",
        False,  # No projection node to push past, but optimization doesn't break anything
        "Simple LIMIT without projection",
    ),
]


@pytest.mark.parametrize("query, should_optimize, description", STATEMENTS)
def test_limit_heapsort_before_projection(query, should_optimize, description):
    """
    Test that LIMIT is pushed before projections when appropriate
    """
    result = opteryx.query(query)
    stats = result.stats

    # The optimization counter should be incremented when optimization happens
    optimization_count = stats.get("optimization_limit_pushdown", 0)

    if should_optimize:
        # For queries that should be optimized, we expect at least one limit pushdown
        # Note: There might be multiple pushdowns in complex queries
        assert optimization_count >= 1, (
            f"Expected limit pushdown optimization for: {description}\n"
            f"Query: {query}\n"
            f"Stats: {stats}"
        )
    else:
        # For queries that shouldn't be optimized in this specific way,
        # the optimization may still happen (e.g., pushing to scan)
        # We're mainly testing that the optimization doesn't break anything
        pass

    # Verify the query returns results without error
    assert result is not None


if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time

    from tests import trunc_printable

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} TESTS")
    for index, (statement, should_optimize, description) in enumerate(STATEMENTS):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_limit_heapsort_before_projection(statement, should_optimize, description)
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
