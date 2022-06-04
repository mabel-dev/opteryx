"""
Tests for the execution of flows. Create a basic flow
and push a payload through it.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.engine.planner.execution_tree import ExecutionTree


def test_linear_execution_tree():
    """
    Test an execution tree where each item has no more than one incoming edge
    """
    tree = ExecutionTree()
    tree.add_operator("p", print)
    tree.add_operator("m", max)
    tree.link_operators("p", "m")

    assert len(tree._nodes) == 2
    assert ["m", "p"] == sorted(tree._nodes.keys())
    assert tree.get_operator("p") == print
    assert len(tree._edges) == 1
    assert tree.get_entry_points() == ["p"]
    assert tree.get_exit_points() == ["m"]
    assert tree.is_acyclic()
    assert tree.get_outgoing_links("p") == ["m"]
    assert tree.get_incoming_links("m") == [("p", None)]

    tree.add_operator("n", min)
    tree.link_operators("m", "n")

    assert len(tree._nodes) == 3
    assert ["m", "n", "p"] == sorted(tree._nodes.keys())
    assert len(tree._edges) == 2


if __name__ == "__main__":  # pragma: no cover
    test_linear_execution_tree()
    print("okay")
