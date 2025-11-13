import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.planner import views


def _plan_text(result):
    return result.stats.get("executed_plan", "") or ""


def _inline_stat(result):
    return result.stats.get("optimization_predicate_pushdown_inline_project", 0)


def test_inline_alias_within_subquery():
    sql = (
        "SELECT DISTINCT mission\n"
        "FROM (SELECT missions, year % 2 == 0 AS even_launch_year FROM $astronauts) AS astro\n"
        "CROSS JOIN UNNEST(missions) AS mission\n"
        "WHERE even_launch_year = TRUE"
    )

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (year % 2 = 0)" in plan
    assert "even_launch_year" not in plan
    assert "READ ($astronauts" in plan
    assert _inline_stat(result) >= 1


def test_inline_alias_from_cte():
    sql = """
WITH astro AS (
    SELECT missions, year % 2 == 0 AS even_launch_year
    FROM $astronauts
)
SELECT DISTINCT mission
FROM astro CROSS JOIN UNNEST(missions) AS mission
WHERE even_launch_year = TRUE
"""

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (year % 2 = 0)" in plan
    assert "even_launch_year" not in plan
    assert "READ ($astronauts" in plan
    assert _inline_stat(result) >= 1


def test_inline_alias_from_view():
    view_name = "inline_alias_astronaut_view"
    views.VIEWS[view_name] = {
        "statement": "SELECT missions, year % 2 == 0 AS even_launch_year FROM $astronauts"
    }

    try:
        result = opteryx.query(
            f"SELECT DISTINCT mission FROM {view_name} CROSS JOIN UNNEST(missions) AS mission WHERE even_launch_year = TRUE"
        )
    finally:
        views.VIEWS.pop(view_name, None)

    plan = _plan_text(result)

    assert "FILTER (year % 2 = 0)" in plan
    assert "even_launch_year" not in plan
    assert "READ ($astronauts" in plan
    assert _inline_stat(result) >= 1


def test_inline_alias_keeps_projected_column():
    sql = (
        "SELECT * FROM (SELECT name LIKE '%a%' AS nom FROM $planets) AS p "
        "WHERE nom = TRUE"
    )

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert result.shape == (4, 1)
    assert result.column_names == ("nom",)
    assert "FILTER (name INSTR 'a')" in plan
    assert _inline_stat(result) >= 1

def test_inline_alias_keeps_projected_column_two():
    sql = (
        "SELECT * FROM (SELECT name LIKE '%a%' AS nom FROM $planets) AS p "
        "WHERE nom IS TRUE"
    )

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert result.shape == (4, 1)
    assert result.column_names == ("nom",)
    assert "FILTER (IsTrue(nom))" in plan, plan

def test_does_not_inline_when_alias_used_directly():
    sql = (
        "SELECT *\n"
        "FROM (SELECT year % 2 == 0 AS even_launch_year FROM $astronauts) AS astro\n"
        "WHERE even_launch_year"
    )

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (even_launch_year)" in plan
    assert _inline_stat(result) == 0


def test_does_not_inline_aggregated_alias():
    sql = (
        "SELECT even_launch_year\n"
        "FROM (SELECT COUNT(*) > 1 AS even_launch_year FROM $astronauts) AS counts\n"
        "WHERE even_launch_year = TRUE"
    )

    result = opteryx.query(sql)
    plan = _plan_text(result)

    assert "FILTER (even_launch_year = True)" in plan
    assert _inline_stat(result) == 0

if __name__ == "__main__":
    from tests import run_tests

    run_tests()
