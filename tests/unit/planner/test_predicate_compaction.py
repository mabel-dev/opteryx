import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _plan_text(result):
    return result.stats.get("executed_plan", "") or ""


def test_predicate_compaction_prefers_strongest_lower_bound():
    result = opteryx.query("SELECT id FROM testdata.planets WHERE id > 4 AND id > 1")

    plan = _plan_text(result)

    assert ") (id > 4)" in plan or "id > 4" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.stats.get("optimization_predicate_compaction", 0) >= 1
    assert result.stats.get("optimization_predicate_compaction_range_simplified", 0) >= 1
    assert result.rowcount == 5


def test_predicate_compaction_collapse_to_equality():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id = 3 AND id > 1 AND id < 9"
    )

    plan = _plan_text(result)

    assert "id = 3" in plan
    assert ">" not in plan
    assert "<" not in plan
    assert result.stats.get("optimization_predicate_compaction_range_simplified", 0) >= 1
    assert result.rowcount == 1


def test_predicate_compaction_contradiction_preserves_schema():
    result = opteryx.query(
        "SELECT * FROM testdata.planets WHERE id > 1 AND id == 0"
    )

    plan = _plan_text(result)

    assert "False" in plan
    assert result.rowcount == 0
    assert len(result.column_names) == 20
    assert result.stats.get("optimization_predicate_compaction_range_simplified", 0) >= 1


def test_predicate_compaction_prefers_strongest_upper_bound():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id < 8 AND id < 5"
    )

    plan = _plan_text(result)

    assert "id < 5" in plan
    assert "id < 8" not in plan.replace("id < 5", "")
    assert result.rowcount == 4


def test_predicate_compaction_handles_mixed_order_bounds():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id < 8 AND id > 1 AND id > 5 AND id < 9"
    )

    plan = _plan_text(result)

    assert "id > 5" in plan
    assert "id < 8" in plan
    assert "id > 1" not in plan.replace("id > 5", "")
    assert "id < 9" not in plan.replace("id < 8", "")
    assert result.rowcount == 2


def test_predicate_compaction_respects_other_column_filters():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id > 1 AND mass > 0 AND id > 4"
    )

    plan = _plan_text(result)

    assert "id > 4" in plan
    assert "mass >" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.stats.get("optimization_predicate_compaction", 0) >= 1
    assert result.rowcount == 5


def test_predicate_compaction_across_subquery_boundary():
    result = opteryx.query(
        "SELECT name FROM (SELECT * FROM testdata.planets WHERE id > 4 AND id > 1) AS p"
    )

    plan = _plan_text(result)

    assert "id > 4" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.stats.get("optimization_predicate_compaction", 0) >= 1
    assert result.rowcount == 5


def test_predicate_compaction_inherited_from_outer_query():
    result = opteryx.query(
        "SELECT * FROM (SELECT id FROM testdata.planets WHERE id > 1) AS p WHERE id > 4"
    )

    plan = _plan_text(result)

    assert "id > 4" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.stats.get("optimization_predicate_compaction", 0) >= 1
    assert result.rowcount == 5


def test_predicate_compaction_with_three_bounds():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id > 0 AND id > 3 AND id > 4"
    )

    plan = _plan_text(result)

    assert "id > 4" in plan
    assert "id > 3" not in plan.replace("id > 4", "")
    assert "id > 0" not in plan.replace("id > 4", "")
    assert result.rowcount == 5


def test_predicate_compaction_handles_different_columns_and_bounds():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id > 1 AND id > 4 AND diameter > 5000"
    )

    plan = _plan_text(result)

    assert "id > 4" in plan
    assert "diameter" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.stats.get("optimization_predicate_compaction", 0) >= 1
    assert result.rowcount == 4


def test_predicate_compaction_handles_alias_qualified_columns():
    result = opteryx.query(
        "SELECT p.id FROM $planets AS p WHERE p.id > 1 AND p.id > 4"
    )

    plan = _plan_text(result)

    assert "id > 4" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert "READ ($planets AS p)" in plan
    assert result.rowcount == 5


def test_predicate_compaction_applied_to_other_dataset():
    result = opteryx.query(
        "SELECT planetId FROM testdata.satellites WHERE planetId > 1 AND planetId > 4"
    )

    plan = _plan_text(result)

    assert "planetId > 4" in plan
    assert "planetId > 1" not in plan.replace("planetId > 4", "")
    assert result.rowcount == 174


def test_predicate_compaction_prefers_exclusive_over_inclusive_lower():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id >= 4 AND id > 4"
    )

    plan = _plan_text(result)

    assert "id > 4" in plan
    assert "id >= 4" not in plan
    assert result.rowcount == 5


def test_predicate_compaction_prefers_exclusive_over_inclusive_upper():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id <= 8 AND id < 8"
    )

    plan = _plan_text(result)

    assert "id < 8" in plan
    assert "id <= 8" not in plan
    assert result.rowcount == 7


def test_predicate_compaction_keeps_equality_with_additional_filters():
    result = opteryx.query(
        "SELECT id FROM testdata.planets WHERE id > 1 AND id = 3 AND diameter < 15_000"
    )

    plan = _plan_text(result)

    assert "id = 3" in plan
    assert ">" not in plan.replace("id = 3", ""), plan
    assert result.rowcount == 1, plan


def test_predicate_compaction_contradiction_inside_subquery():
    result = opteryx.query(
        "SELECT * FROM (SELECT * FROM testdata.planets WHERE id > 1 AND id == 0) AS p"
    )

    plan = _plan_text(result)

    assert "False" in plan
    assert result.rowcount == 0
    assert len(result.column_names) == 20

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()