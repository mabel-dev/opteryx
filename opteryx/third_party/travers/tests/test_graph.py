# isort: skip_file
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))


def build_graph():
    """
    Create a graph for testing
    """
    from opteryx.third_party.travers import Graph

    g = Graph()

    g.add_node("Bindoon", {"node_type": "Locality"})
    g.add_node("Gingin", {"node_type": "Locality"})
    g.add_node("Toodyay", {"node_type": "Locality"})

    g.add_node("Sharlene", {"node_type": "Person"})
    g.add_node("Ceanne", {"node_type": "Person"})
    g.add_node("Lainie", {"node_type": "Person"})

    g.add_node("Hungry Jacks", {"node_type": "Restaurant"})
    g.add_node("Chicken Treat", {"node_type": "Restaurant"})
    g.add_node("Kailis Bros", {"node_type": "Restaurant"})

    g.add_node("Saturn", {"node_type": "Planet"})

    g.add_edge("Sharlene", "Bindoon", "Lives In")
    g.add_edge("Ceanne", "Gingin", "Lives In")
    g.add_edge("Lainie", "Toodyay", "Lives In")

    g.add_edge("Hungry Jacks", "Bindoon", "Located In")
    g.add_edge("Hungry Jacks", "Gingin", "Located In")
    g.add_edge("Chicken Treat", "Gingin", "Located In")
    g.add_edge("Chicken Treat", "Toodyay", "Located In")
    g.add_edge("Kailis Bros", "Toodyay", "Located In")

    g.add_edge("Sharlene", "Ceanne", "Sister")
    g.add_edge("Ceanne", "Sharlene", "Sister")
    g.add_edge("Sharlene", "Lainie", "Daughter")
    g.add_edge("Ceanne", "Lainie", "Daughter")
    g.add_edge("Lainie", "Sharlene", "Mother")
    g.add_edge("Lainie", "Ceanne", "Mother")

    g.add_edge("Sharlene", "Hungry Jacks", "Likes")
    g.add_edge("Ceanne", "Kailis Bros", "Likes")
    g.add_edge("Lainie", "Kailis Bros", "Likes")

    return g


def graph_is_as_expected(graph):
    assert sorted(graph.nodes()) == [
        "Bindoon",
        "Ceanne",
        "Chicken Treat",
        "Gingin",
        "Hungry Jacks",
        "Kailis Bros",
        "Lainie",
        "Saturn",
        "Sharlene",
        "Toodyay",
    ]
    assert len(list(graph.edges())) == 17


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

    assert len(summ.nodes()) == 3
    assert len(list(summ.edges())) == 4

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


def test_incoming_edges():
    graph = build_graph()
    incoming = graph.ingoing_edges("Bindoon")
    assert len(incoming) == 2
    sources = [s for s, t, r in incoming]
    assert "Sharlene" in sources
    assert "Hungry Jacks" in sources


def test_node_attributes():
    graph = build_graph()
    attrs = graph["Bindoon"]
    assert attrs["node_type"] == "Locality"


def test_edge_deletion():
    graph = build_graph()
    graph.remove_edge("Sharlene", "Bindoon", "Lives In")
    assert ("Sharlene", "Bindoon", "Lives In") not in graph.edges()


def test_node_deletion():
    graph = build_graph()
    graph.remove_node("Bindoon", True)
    assert "Bindoon" not in graph.nodes()
    assert ("Sharlene", "Bindoon", "Lives In") not in graph.edges()


def test_update_existing_edge():
    graph = build_graph()

    # Add an edge
    graph.add_edge("Sharlene", "Bindoon", "friend")

    # Update the edge
    graph.add_edge("Sharlene", "Bindoon", "best friend")

    # Verify the edge was updated
    edges = graph.outgoing_edges("Sharlene")
    updated = False
    for source, target, relationship in edges:
        if target == "Bindoon" and relationship == "best friend":
            updated = True
            break

    assert updated, "The edge relationship was not updated correctly"


if __name__ == "__main__":  # pragma: no cover
    test_graph()
    test_outgoing_edges()
    test_epitomize()
    test_bfs()
    test_incoming_edges()
    test_node_attributes()
    test_edge_deletion()
    test_node_deletion()
    test_update_existing_edge()
    print("okay")
