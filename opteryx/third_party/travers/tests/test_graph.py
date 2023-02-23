# isort: skip_file
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from opteryx.third_party.travers.tests.graph_data import (
    build_graph,
    graph_is_as_expected,
)


def test_graph():
    graph = build_graph()
    graph_is_as_expected(graph)


def test_outgoing_edges():
    graph = build_graph()

    outgoing = graph.outgoing_edges("Sharlene")

    assert len(outgoing) == 4

    sources = [s for s, t, r in outgoing]
    targets = [t for s, t, r in outgoing]
    relationships = [r for s, t, r in outgoing]

    assert set(sources) == {"Sharlene"}
    assert sorted(targets) == ["Bindoon", "Ceanne", "Hungry Jacks", "Lainie"]
    assert sorted(relationships) == ["Daughter", "Likes", "Lives In", "Sister"]


def test_epitomize():
    graph = build_graph()

    summ = graph.epitomize()
    # are the node and edge counts right?
    assert len(summ.nodes()) == 3
    assert len(list(summ.edges())) == 6

    assert sorted(summ.nodes()) == ["Locality", "Person", "Restaurant"]


def test_bfs():
    graph = build_graph()

    # this should exclude the node with no edges
    bfs = graph.breadth_first_search("Saturn")
    assert len(bfs) == 0

    bfs = graph.breadth_first_search("Sharlene")
    assert len(bfs) == 15
    assert "Saturn" not in bfs

    bfs = graph.breadth_first_search("Sharlene", 0)
    assert len(bfs) == 0

    bfs = graph.breadth_first_search("Sharlene", 1)
    assert len(bfs) == 4

    bfs = graph.breadth_first_search("Sharlene", 2)
    assert len(bfs) == 14


if __name__ == "__main__":  # pragma: no cover
    test_graph()
    test_outgoing_edges()
    test_epitomize()
    test_bfs()

    print("okay")
