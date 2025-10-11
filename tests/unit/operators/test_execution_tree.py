"""
Test basic functionality of the execution tree
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models.physical_plan import PhysicalPlan


def test_execution_tree():
    et = PhysicalPlan()
    et.add_node("a", None)
    et.add_node("b", None)
    et.add_edge("a", "b", "forwards")

    assert et.is_acyclic()
    assert et.get_entry_points() == ["a"]
    assert et.get_exit_points() == ["b"]

    et.add_node("c", None)
    et.add_edge("b", "c", "forward")
    et.add_edge("c", "a", "forward")

    assert not et.is_acyclic()
    assert et.get_entry_points() == []
    assert et.get_exit_points() == []


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
