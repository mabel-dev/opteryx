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

    def identify_flows(self):
        """
        Identify chains of operations that can be executed together as flows/subplans.
        
        A flow is a sequence of operations that can be sent to a worker to execute
        together without needing to report back interim snapshots. Flows break at:
        - Stateful nodes (require accumulation across morsels)
        - Join nodes (require coordination between legs)
        - Branch points (nodes with multiple children)
        - Merge points (nodes with multiple parents)
        
        Stateful/join nodes are NOT included in flows - they act as boundaries.
        
        This method annotates each node with a flow_id to indicate which flow it belongs to.
        """
        from orso.tools import random_string
        
        # Initialize flow_id for all nodes
        for nid in self.nodes():
            node = self[nid]
            node.flow_id = None
        
        visited = set()
        flow_counter = 0
        
        # Process nodes in depth-first order
        for nid, node in self.depth_first_search_flat():
            if nid in visited:
                continue
            
            visited.add(nid)
            
            # Stateful nodes and joins don't belong to flows - they are flow boundaries
            if not node.is_stateless or node.is_join:
                # Mark as visited but don't assign to a flow
                continue
            
            # Check if this node can start or continue a flow
            incoming = self.ingoing_edges(nid)
            
            # Determine if we should start a new flow or continue existing one
            should_start_new_flow = True
            parent_flow_id = None
            
            if len(incoming) == 1:
                parent_nid = incoming[0][0]
                parent_node = self[parent_nid]
                parent_outgoing = self.outgoing_edges(parent_nid)
                
                # Can continue parent's flow if:
                # - Parent is stateless (in a flow)
                # - Parent has only one child (no branch)
                # - Parent is not a join
                if (parent_node.is_stateless and 
                    not parent_node.is_join and 
                    len(parent_outgoing) == 1 and
                    parent_node.flow_id is not None):
                    should_start_new_flow = False
                    parent_flow_id = parent_node.flow_id
            
            # Start new flow or continue parent's flow
            if should_start_new_flow:
                current_flow_id = f"flow_{flow_counter}"
                flow_counter += 1
            else:
                current_flow_id = parent_flow_id
            
            # Assign this node to the flow
            node.flow_id = current_flow_id
        
        return flow_counter

    def sensors(self):
        readings = {}
        for nid in self.nodes():
            node = self[nid]
            readings[node.identity] = node.sensors()
        return readings

    def __del__(self):
        pass
