"""Ensure redundant operators strategy handles aggregates."""

import opteryx


def test_redundant_project_removed_after_aggregate() -> None:
    """An aggregate followed by a projection should be optimized away."""
    result = opteryx.query("SELECT total FROM (SELECT COUNT(*) AS total FROM $planets)")
    stats = result.stats

    assert stats.get("optimization_remove_redundant_operators_project", 0) >= 1
