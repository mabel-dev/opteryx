import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _plan_text(result):
    return result.stats.get("executed_plan", "") or ""


def test_predicate_compaction_compacts_before_join_scan():
    sql = (
        "SELECT p.name FROM $planets AS p "
        "INNER JOIN testdata.satellites AS s ON p.id = s.planetId "
        "WHERE p.id > 1 AND p.id > 4"
    )

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (id > 4)" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.stats.get("optimization_predicate_compaction", 0) >= 1

    baseline = opteryx.query(
        "SELECT p.name FROM $planets AS p INNER JOIN testdata.satellites AS s ON p.id = s.planetId WHERE p.id > 4"
    )
    assert result.rowcount == baseline.rowcount


def test_predicate_compaction_in_nested_subquery():
    sql = """
        SELECT COUNT(*)
        FROM (
            SELECT id FROM $planets WHERE id > 1 AND id > 4
        ) AS sub
    """

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (id > 4)" in plan
    assert "id > 1" not in plan.replace("id > 4", "")
    assert result.stats.get("optimization_predicate_compaction", 0) >= 1
    assert result.fetchall() == [(5,)]
