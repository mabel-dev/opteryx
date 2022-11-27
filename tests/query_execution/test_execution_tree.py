"""
Test basic functionality of the execution tree
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models.execution_tree import ExecutionTree


def test_execution_tree():

    et = ExecutionTree()
    et.add_operator("a", None)
    et.add_operator("b", None)
    et.link_operators("a", "b", "forwards")

    assert et.is_acyclic()
    assert et.get_entry_points() == ["a"]
    assert et.get_exit_points() == ["b"]

    et.add_operator("c", None)
    et.link_operators("b", "c", "forward")
    et.link_operators("c", "a", "forward")

    assert not et.is_acyclic()
    assert et.get_entry_points() == []
    assert et.get_exit_points() == []


if __name__ == "__main__":  # pragma: no cover

    test_execution_tree()
