# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Physical Plan is a tree of nodes that represent the execution plan for a query.
"""

from typing import Optional

from opteryx.exceptions import InvalidInternalStateError
from opteryx.third_party.travers import Graph


class PhysicalPlan(Graph):
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.
    """

    def depth_first_search_flat(
        self, node: Optional[str] = None, visited: Optional[set] = None
    ) -> list:
        """
        Returns a flat list representing the depth-first traversal of the graph with left/right ordering.

        We do this so we always evaluate the left side of a join before the right side. It technically
        doesn't need the entire plan flattened DFS-wise, but this is what we are doing here to achieve
        the outcome we're after.
        """
        if node is None:
            node = self.get_exit_points()[0]

        if visited is None:
            visited = set()

        visited.add(node)

        # Collect this node's information in a flat list format
        traversal_list = [
            (
                node,
                self[node],
            )
        ]

        # Sort neighbors based on relationship to ensure left, right, then unlabelled order
        neighbors = sorted(self.ingoing_edges(node), key=lambda x: (x[2] == "right", x[2] == ""))

        # left semi and anti joins we hash the right side first, usually we want the left side first
        if self[node].is_join and self[node].join_type in ("left anti", "left semi"):
            neighbors.reverse()

        # Traverse each child, prioritizing left, then right, then unlabelled
        for neighbor, _, _ in neighbors:
            if neighbor not in visited:
                child_list = self.depth_first_search_flat(neighbor, visited)
                traversal_list.extend(child_list)

        return traversal_list

    def label_join_legs(self):
        # add the left/right labels to the edges coming into the joins
        joins = ((nid, node) for nid, node in self.nodes(True) if node.is_join)
        for nid, join in joins:
            for provider, provider_target, provider_relation in self.ingoing_edges(nid):
                reader_edges = {
                    (source, target, relation)
                    for source, target, relation in self.breadth_first_search(
                        provider, reverse=True
                    )
                }  # if hasattr(self[target], "uuid")}
                if hasattr(self[provider], "uuid"):
                    reader_edges.add((provider, provider_target, provider_relation))

                for s, t, r in reader_edges:
                    node = self[s]
                    if not hasattr(node, "uuid"):
                        continue
                    if node.uuid in join.left_readers:
                        self.add_edge(provider, nid, "left")
                    elif node.uuid in join.right_readers:
                        self.add_edge(provider, nid, "right")

            tester = self.breadth_first_search(nid, reverse=True)
            if not any(r == "left" for s, t, r in tester):
                raise InvalidInternalStateError("Unable to determine LEFT side of join.")
            if not any(r == "right" for s, t, r in tester):
                raise InvalidInternalStateError("Join has no RIGHT leg")

    def sensors(self):
        readings = {}
        for nid in self.nodes():
            node = self[nid]
            readings[node.identity] = node.sensors()
        return readings

    def __del__(self):
        pass
