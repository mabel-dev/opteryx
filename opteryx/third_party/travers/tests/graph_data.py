import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from travers import Graph


def build_graph():
    """
    Create a graph for testing
    """

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
