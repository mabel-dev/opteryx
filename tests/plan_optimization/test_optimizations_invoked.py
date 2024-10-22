"""
The best way to test a SQL engine is to throw queries at it.

This tests the various format readers.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest

import opteryx


# fmt:off
STATEMENTS = [
        ("SELECT * FROM $planets WHERE NOT id != 4", "optimization_boolean_rewrite_inversion"),
        ("SELECT * FROM $planets WHERE id = 4 + 4", "optimization_constant_fold_expression"),
        ("SELECT * FROM $planets WHERE id * 0 = 1", "optimization_constant_fold_reduce"),
        ("SELECT id ^ 1 = 1 FROM $planets LIMIT 10", "optimization_limit_pushdown"),
        ("SELECT name FROM $astronauts WHERE name = 'Neil A. Armstrong'", "optimization_predicate_pushdown")
    ]
# fmt:on


@pytest.mark.parametrize("statement, flag", STATEMENTS)
def test_optimization_invoked(statement, flag):
    """
    Test an battery of statements
    """

    result = opteryx.query(statement)
    stats = result.stats

    assert stats.get(flag) is not None, stats


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} OPTIMIZER TESTS")
    for statement, flag in STATEMENTS:
        print(statement)
        test_optimization_invoked(statement, flag)

    print("✅ okay")
