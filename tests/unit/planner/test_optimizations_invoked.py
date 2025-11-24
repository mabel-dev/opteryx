"""
The best way to test a SQL engine is to throw queries at it.

This tests the various format readers.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
import pytest

import opteryx
from opteryx.utils.formatter import format_sql
from tests import trunc_printable

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
        ("SELECT CASE WHEN surface_pressure IS NULL THEN -100.00 ELSE surface_pressure END FROM $planets", "optimization_predicate_rewriter_case_to_ifnull"),
        ("SELECT * FROM $satellites INNER JOIN $planets ON planet_id = $planets.id", "optimization_inner_join_smallest_table_left"),
        ("SELECT name FROM $astronauts WHERE 'MIT' = ANY(alma_mater) OR 'Stanford' = ANY(alma_mater) OR 'Harvard' = ANY(alma_mater)", "optimization_predicate_rewriter_anyeq_to_contains"),
        ("SELECT COUNT(*) FROM $planets WHERE STARTS_WITH(name, 'M')", "optimization_predicate_rewriter_starts_with_to_like"),
        ("SELECT COUNT(*) FROM $planets WHERE ENDS_WITH(name, 's')", "optimization_predicate_rewriter_ends_with_to_like"),
        ("SELECT name FROM $astronauts WHERE 'Apollo 13' = ANY(missions) AND 'Gemini 8' = ANY(missions)", "optimization_predicate_rewriter_anyeq_to_contains_all"),
        # New boolean simplification tests
        ("SELECT * FROM $planets WHERE id > 5 AND name = 'Earth' AND id < 10", "optimization_boolean_rewrite_and_flatten"),  # AND chain flattening
        # De Morgan's n-ary tests (NOT of multiple OR conditions) - creates multiple AND predicates for pushdown
        ("SELECT * FROM $planets WHERE NOT(id = 1 OR id = 2 OR id = 3)", "optimization_boolean_rewrite_demorgan_nary"),  # NOT(A OR B OR C) => A!=1 AND A!=2 AND A!=3
        ("SELECT * FROM $planets WHERE NOT(id = 1 OR id = 2 OR id = 3 OR id = 4)", "optimization_boolean_rewrite_demorgan_nary"),  # 4 conditions
        # Predicate compaction tests - multiple predicates on same column get compacted to most restrictive
        ("SELECT * FROM $planets WHERE id > 1 AND id > 3", "optimization_predicate_compaction"),  # id > 1 AND id > 3 => id > 3
        ("SELECT * FROM $planets WHERE id < 8 AND id < 5", "optimization_predicate_compaction"),  # id < 8 AND id < 5 => id < 5
        ("SELECT * FROM $planets WHERE id > 1 AND id < 8 AND id > 3 AND id < 7", "optimization_predicate_compaction"),  # Multiple bounds compacted
        # Correlated filters test - filters created based on join statistics
        #("SELECT s.name FROM $satellites s INNER JOIN $planets p ON s.planetId = p.id", "optimization_inner_join_correlated_filter"),  # Correlated filter on join
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
