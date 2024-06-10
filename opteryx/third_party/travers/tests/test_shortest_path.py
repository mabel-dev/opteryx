# isort: skip_file
"""
Test basic functionality of the execution tree
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.third_party.travers import Graph


def test_shortest_path():
    graph = Graph()

    # Create a simple graph structure
    graph.add_edge("A", "B")
    graph.add_edge("A", "C")
    graph.add_edge("B", "D")
    graph.add_edge("C", "D")
    graph.add_edge("D", "E")

    # Test 1: Path between directly connected nodes
    assert graph.shortest_path("A", "B") == ["A", "B"], graph.shortest_path("A", "B")

    # Test 2: Path between nodes with multiple options, but one is shortest
    assert graph.shortest_path("A", "D") in (["A", "B", "D"], ["A", "C", "D"])

    # Test 3: Path between distant nodes
    assert graph.shortest_path("A", "E") in (["A", "B", "D", "E"], ["A", "C", "D", "E"])

    # Test 4: Path from a node to itself
    assert graph.shortest_path("A", "A") == ["A"]

    # Test 5: No path exists
    graph.add_node("F", "F")
    assert graph.shortest_path("A", "F") == []

    # Test 6: Path between nodes when there are isolated nodes in the graph
    graph.add_node("G", "G")
    graph.add_node("H", "H")
    assert graph.shortest_path("A", "E") in (["A", "B", "D", "E"], ["A", "C", "D", "E"])


def test_shortest_path_empty_graph():
    graph = Graph()

    assert graph.shortest_path("A", "B") == []


def test_shortest_path_missing_node():
    graph = Graph()
    graph.add_node("A", "A")

    assert graph.shortest_path("A", "B") == []


if __name__ == "__main__":  # pragma: no cover
    test_shortest_path()
    test_shortest_path_empty_graph()
    test_shortest_path_missing_node()

    print("okay")
