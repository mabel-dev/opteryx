"""
travers

(C) 2023 Justin Joyce.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pathlib import Path
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

import orjson

from opteryx.exceptions import MissingDependencyError


def print_tree_inner(tree, prefix="", last=True):
    """
    Prints a nested dictionary as an ascii tree
    """

    yield prefix
    if last:
        yield "└─ "
        prefix += "   "
    else:
        yield "├─ "
        prefix += "│  "

    yield str(tree["node"]) + "\n"

    # Recursively print the children
    count = len(tree["children"])
    for i, child in enumerate(tree["children"]):
        last = i == count - 1
        yield from print_tree_inner(child, prefix, last)


class Graph(object):
    """
    Graph object, optimized for traversal.

    Edges are stored in a dictionary, the key is the source node to speed up
    finding outgoing edges. The Edges only have three pieces of data:
        - the source node (the key)
        - the target node
        - the relationship
    The target and the relationship are stored as a tuple, the edge dictionary
    stores lists of tuples.

    """

    __slots__ = ("_nodes", "_edges")

    def __init__(self):
        """
        Directed Graph.
        """
        self._nodes = {}
        self._edges = {}

    def __bool__(self) -> bool:
        return len(self._nodes) != 0 or len(self._edges) != 0

    def save(self, graph_path):  # pragma: nocover
        """
        Persist a graph to storage. It saves nodes and edges to separate files.

        Parameters:
            graph_path: string
                The folder ?to save the node and edge files to
        """
        path = Path(graph_path)
        path.mkdir(exist_ok=True)

        with open(path / "edges.jsonl", "wb") as edge_file:
            for source, target, relationship in self.edges():
                edge_record = {
                    "source": source,
                    "target": target,
                    "relationship": relationship,
                }
                edge_file.write(orjson.dumps(edge_record) + b"\n")
        with open(path / "nodes.jsonl", "wb") as node_file:
            for nid, attr in self.nodes(data=True):
                node_file.write(orjson.dumps({"nid": nid, "attributes": attr}) + b"\n")

    def add_edge(self, source: str, target: str, relationship: Optional[str] = None):
        """
        Add edge to the graph

        Note:
            This does not create an edge if either node does not already exist.
            This does not create an edge if either node is None.

        Parameters:
            source: string
                The source node
            target: string
                The target node
            relationship: string
                The relationship between the source and target nodes
        """
        if source is None or target is None:
            print("Trying to create edge with undefined nodes")
            return False

        # Check for existing edges and add the new one
        existing_edges = list(self._edges.get(source, ()))

        # Avoid adding duplicate edges
        edge_to_add = (target, relationship)
        if edge_to_add not in existing_edges:
            existing_edges.append(edge_to_add)

        self._edges[source] = tuple(existing_edges)

    def add_node(self, nid: str, node):
        """
        Add node to the graph

        Parameters:
            nid: string
                The Node ID for the node (unique)
            attributes: dictionary (optional)
                The attributes of the node
        """
        self._nodes[nid] = node

    def nodes(self, data=False):
        """
        The nodes which comprise the graph

        Parameters:
            data: boolean (optional)
                if True return the details of the nodes, if False just return
                the list of node IDs

        Returns:
            List
        """
        if data:
            return self._nodes.items()
        return list(self._nodes.keys())

    def edges(self):
        """
        The edges which comprise the graph

        Returns:
            Generator of Tuples of (Source, Target and Relationship)
        """
        for source, records in self._edges.items():
            yield from ((source, target, relationship) for target, relationship in records)

    def breadth_first_search(self, source: str, depth: int = 100):  # pragma: nocover
        """
        Search a tree for nodes we can walk to from a given node.

        Parameters:
            source: string
                The node to walk from
            depth: integer
                The maximum distance to walk from source
        Returns:
        """
        from collections import deque

        visited = {source}
        queue = deque([(source, 0)])

        traversed_edges = []

        while queue:
            current_node, current_depth = queue.popleft()

            if current_depth < depth:
                for edge in self.outgoing_edges(current_node):
                    _, target, _ = edge

                    # Add the edge to the traversed edges list
                    traversed_edges.append(edge)

                    if target not in visited:
                        visited.add(target)
                        queue.append((target, current_depth + 1))

        return traversed_edges

    def depth_first_search(
        self, node: Optional[str] = None, visited: Optional[set] = None, depth: int = 0
    ):
        """
        Returns a nested dictionary representing the graph as a tree
        """
        if node is None:
            node = self.get_exit_points()[0]

        if visited is None:
            visited = set()

        visited.add(node)

        tree: dict = {
            "type": str(self[node].node_type),
            "node": str(self[node]),
            "name": node,
            "depth": depth,
            "children": [],
        }

        for neighbor, _, relationship in self.ingoing_edges(node):
            if neighbor not in visited:
                child = self.depth_first_search(neighbor, visited, depth + 1)
                child["relationship"] = relationship
                tree["children"].append(child)  # type:ignore

        return tree

    def outgoing_edges(self, source) -> List[Tuple]:
        """
        Get the list of edges traversable from a given node.

        Parameters:
            source: string
                The node to get the outgoing edges for

        Returns:
            Set of Tuples (Source, Target, Relationship)
        """
        return [(source, t, r) for t, r in self._edges.get(source, tuple())]

    def ingoing_edges(self, target) -> List[Tuple]:
        """
        Get the list of edges which can traverse to a given node.

        Parameters:
            target: string
                The node to get the incoming edges for

        Returns:
            Set of Tuples (Source, Target, Relationship)
        """
        return [(s, t, r) for s, t, r in self.edges() if t == target]

    def is_acyclic(self):
        """
        Test if the Graph is acyclic
        """
        # cycle over the graph removing a layer of exits each cycle
        # if we have nodes but no exists, we're cyclic

        # rebuild the edge information
        my_edges = list(self.edges())

        while len(my_edges) > 0:
            # find all of the exits
            sources = {source for source, target, direction in my_edges}
            exits = {target for source, target, direction in my_edges if target not in sources}

            if len(exits) == 0:
                return False

            # remove the exits
            new_edges = [
                (source, target, direction)
                for source, target, direction in my_edges
                if target not in exits
            ]
            my_edges = new_edges
        return True

    def shortest_path(self, start: str, end: str) -> List[str]:
        """
        Compute the shortest path from start to end node.

        Parameters:
            start: string
                The starting node ID
            end: string
                The target node ID

        Returns:
            List of node IDs from start to end node that represent the shortest path.
            Returns an empty list if no path is found.
        """

        from collections import deque

        visited = set()
        queue = deque([(start, [start])])  # Each item in the queue is a tuple (node, path_so_far)

        while queue:
            node, path = queue.popleft()

            if node == end:
                return path  # Found a path to the end node

            if node not in visited:
                visited.add(node)

                for _, neighbor, _ in self.outgoing_edges(node):
                    if neighbor == end:
                        path.append(neighbor)
                        return path
                    if neighbor not in visited:
                        new_path = list(path)
                        new_path.append(neighbor)
                        queue.append((neighbor, new_path))

        return []  # No path found

    def get_entry_points(self):
        """
        Get nodes in the Graph with no incoming edges.
        """
        if len(self._nodes) == 1:
            return list(self._nodes.keys())

        edges = list(self.edges())
        targets = {target for _, target, _ in edges}
        sources = {source for source, _, _ in edges}
        return sorted(sources - targets)

    def get_exit_points(self):
        """
        Get nodes in the Graph with no outgoing edges.
        """
        if len(self._nodes) == 1:
            return list(self._nodes.keys())

        edges = list(self.edges())
        targets = {target for _, target, _ in edges}
        sources = set(self._edges.keys())
        return sorted(targets - sources)

    def remove_node(self, nid, heal: bool = False):
        """
        Remove a node.

        Parameters:
            heal: boolean
                Join the incoming and outgoing connections for the removed node
                to each other to keep the Graph intact
        """

        # remove the node
        self._nodes.pop(nid, None)

        if heal:
            # link the nodes each side of the node being removed
            out_going = self.outgoing_edges(nid)
            in_coming = self.ingoing_edges(nid)

            # remove edges where the node is the source
            if nid in self._edges:
                del self._edges[nid]

            # remove the edges where the node is the target
            for source, records in self._edges.items():
                self._edges[source] = [
                    (target, relationship) for target, relationship in records if target != nid
                ]
            self._edges = {k: v for k, v in self._edges.items() if len(v) > 0}

            # wire up the old incoming and outgoing nodes, cartesian style
            for out_nid in out_going:
                for in_nid in in_coming:
                    self.add_edge(in_nid[0], out_nid[1], in_nid[1])  # type:ignore

    def remove_edge(self, source, target, relationship):
        """
        Remove an edge from the graph.
        Args:
        - source (str): The source node of the edge.
        - target (str): The target node of the edge.
        - relationship (str): The relationship label of the edge.
        """
        if source not in self._edges:
            return
        edge_to_remove = (target, relationship)
        if source in self._edges and edge_to_remove in self._edges[source]:
            working_set = list(self._edges[source])
            working_set.remove(edge_to_remove)
            self._edges[source] = tuple(working_set)
            if not self._edges[source]:  # If no edges left for the source
                del self._edges[source]

    def insert_node_before(self, nid, node, before_nid):
        """rewrite the plan putting the new node before a given node"""
        # add the new node to the plan
        self.add_node(nid, node)
        # change all the edges that were going into the old nid to the new one

        for source, records in self._edges.items():
            new_records = []
            for target, relationship in records:
                if target != before_nid:
                    new_records.append(
                        (
                            target,
                            relationship,
                        )
                    )
                else:
                    new_records.append(
                        (
                            nid,
                            relationship,
                        )
                    )
                self._edges[source] = new_records
        # add an edge from the new nid to the old one
        self.add_edge(nid, before_nid)

    def insert_node_after(self, nid, node, after_nid):
        """rewrite the plan putting the new node after a given node"""
        # add the new node to the plan
        self.add_node(nid, node)
        # change all the edges that were coming from the old nid to the new one
        if after_nid in self._edges:
            self._edges[nid] = self._edges.pop(after_nid)
        # add an edge from the new nid to the old one
        self.add_edge(after_nid, nid)

    def to_networkx(self):  # pragma: nocover
        """
        Convert a travers graph to a NetworkX graph
        """
        try:
            import networkx as nx  # type:ignore
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        g = nx.DiGraph()
        for s, t, r in self.edges():
            g.add_edge(s, t, relationship=r)
        for node, attribs in self.nodes(True):
            g.add_node(node, **attribs)
        return g

    def epitomize(self):  # pragma: nocover
        """
        Summarize a Graph by reducing to only the node_types and relationships
        """
        g = Graph()
        for s, t, r in self.edges():
            node1 = self[s]
            node2 = self[t]
            if node1 and node2:
                g.add_edge(node1.get("node_type"), node2.get("node_type"), r)
            if node1:
                g.add_node(node1.get("node_type"), {"node_type": node1.get("node_type")})
            if node2:
                g.add_node(node2.get("node_type"), {"node_type": node2.get("node_type")})
        return g

    def __repr__(self):
        return f"Graph - {len(list(self.nodes()))} nodes, {len(list(self.edges()))} edges"

    def __len__(self):
        return len(list(self.nodes()))

    def __getitem__(self, nid):
        return self._nodes.get(nid, None)

    def __setitem__(self, nid, node):
        if not nid in self._nodes:
            raise ValueError("Cannot create nodes with [] syntax")
        self._nodes[nid] = node

    def __add__(self, other):
        self._edges.update(other._edges)
        self._nodes.update(other._nodes)
        return self

    def __contains__(self, nid: str) -> bool:
        return nid in self._nodes

    def draw(self):
        tree = self.depth_first_search()
        return "".join(print_tree_inner(tree))

    def copy(self) -> "Graph":
        """
        Intelligently make a copy of this Graph, handling situations where Deepcopy
        does not work.
        """
        import copy

        def _inner_copy(obj: Any) -> Any:
            """
            Create an independent inner copy of the given object.

            Parameters:
                obj: Any
                    The object to be deep copied.

            Returns:
                Any: The new, independent deep copy.
            """
            if isinstance(obj, list):
                return [_inner_copy(item) for item in obj]
            if isinstance(obj, tuple):
                return tuple(_inner_copy(item) for item in obj)
            if isinstance(obj, set):
                return {_inner_copy(item) for item in obj}
            if isinstance(obj, dict):
                return {key: _inner_copy(value) for key, value in obj.items()}
            if hasattr(obj, "copy"):
                return obj.copy()
            try:
                return copy.deepcopy(obj)
            except:
                return obj

        graph = Graph()
        graph._nodes = _inner_copy(self._nodes)
        graph._edges = self.copy_edges()

        return graph

    def copy_edges(self):
        """
        Creates an independent copy of the edges in the graph.

        Returns:
            A new dictionary representing the edges in the graph.
        """
        new_edges: dict = {}
        for source, target, relationship in self.edges():
            if source not in new_edges:
                new_edges[source] = []
            new_edges[source].append((target, relationship))
        return new_edges

    def trace_to_root(self, nid: str) -> list:
        """
        Traces the path from this node to the root of the tree, recording each node along the way.

        Args:
            tree: The tree structure containing the nodes and edges.
        """
        route = []
        current_node_id = nid
        while True:
            # Get the node before the current node
            outgoing_edges = self.outgoing_edges(current_node_id)
            if not outgoing_edges:
                break  # Reached the root

            # Assuming the first element of the first tuple in ingoing_edges is the previous node's ID
            previous_node_id = outgoing_edges[0][1]

            # Record this node in the chain
            route.append(previous_node_id)

            # Move to the previous node
            current_node_id = previous_node_id

        return route
