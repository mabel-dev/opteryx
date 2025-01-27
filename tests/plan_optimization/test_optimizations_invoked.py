"""
The best way to test a SQL engine is to throw queries at it.

This tests the various format readers.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest

import opteryx
from opteryx.utils.formatter import format_sql
from tests.tools import trunc_printable

# fmt:off
STATEMENTS = [
        ("SELECT * FROM $planets WHERE NOT id != 4", "optimization_boolean_rewrite_inversion"),
        ("SELECT * FROM $planets WHERE id = 4 + 4", "optimization_constant_fold_expression"),
        ("SELECT * FROM $planets WHERE id * 0 = 1", "optimization_constant_fold_reduce"),
        ("SELECT id ^ 1 = 1 FROM $planets LIMIT 10", "optimization_limit_pushdown"),
        ("SELECT name FROM $astronauts WHERE name = 'Neil A. Armstrong'", "optimization_predicate_pushdown"),
        ("SELECT name FROM $planets WHERE name LIKE '%'", "optimization_constant_fold_reduce"), # rewritten to `name is not null`
        ("SELECT name FROM $planets WHERE name ILIKE '%'", "optimization_constant_fold_reduce"), # rewritten to `name is not null`
        ("SELECT name FROM $planets WHERE name ILIKE '%th%'", "optimization_predicate_rewriter_replace_like_with_in_string"), 
        ("SELECT name FROM $planets WHERE NOT name NOT ILIKE '%th%'", "optimization_boolean_rewrite_inversion"),
        ("SELECT * FROM $planets WHERE NOT name != 'Earth'", "optimization_boolean_rewrite_inversion"),
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
    import shutil
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} OPTIMIZER TESTS")

    width = shutil.get_terminal_size((80, 20))[0] - 15
    for index, (statement, flag) in enumerate(STATEMENTS):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        test_optimization_invoked(statement, flag)
        print()

    print("✅ okay")
