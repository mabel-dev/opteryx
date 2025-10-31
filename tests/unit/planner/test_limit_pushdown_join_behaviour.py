import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx  # noqa: E402
import pytest  # noqa: E402


def _materialize(query: str):
    cursor = opteryx.query(query)
    cursor.materialize()
    return cursor


def test_limit_pushdown_left_outer_join():
    query = (
        "SELECT s.name FROM testdata.satellites AS s "
        "LEFT JOIN testdata.planets AS p ON s.planetId = p.id LIMIT 5;"
    )
    cursor = _materialize(query)
    plan_lines = cursor.stats["executed_plan"].splitlines()
    scan_line = next(
        line for line in plan_lines if "READ (testdata.satellites AS s)" in line
    )
    assert "LIMIT 5" in scan_line, cursor.stats["executed_plan"]
    assert cursor.stats["rows_read"] <= 14, cursor.stats


def test_limit_pushdown_cross_join_prefers_smaller_side():
    query = (
        "SELECT * FROM testdata.planets AS p CROSS JOIN testdata.satellites AS s LIMIT 5;"
    )
    cursor = _materialize(query)
    plan_lines = cursor.stats["executed_plan"].splitlines()
    scan_line = next(
        line for line in plan_lines if "READ (testdata.planets AS p)" in line
    )
    assert "LIMIT 5" in scan_line, cursor.stats["executed_plan"]
    assert cursor.stats["rows_read"] <= 182, cursor.stats

if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])