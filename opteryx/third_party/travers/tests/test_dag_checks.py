# isort: skip_file
"""
Test basic functionality of the execution tree
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.third_party.travers import Graph


def test_dag_checks():
    et = Graph()
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
    test_dag_checks()
