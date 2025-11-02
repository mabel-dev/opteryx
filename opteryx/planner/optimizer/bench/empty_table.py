# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Empty Table Elimination

Type: Structural
Goal: Replace FILTER(FALSE) + subtree with empty data source

When predicate compaction or other optimizations detect contradictory
conditions (e.g., column = 'A' AND column = 'B'), they produce FILTER(FALSE).
Rather than executing the entire subtree and filtering out all rows, we
detect these impossible conditions and replace the FILTER node and its
subtree with an empty FunctionDataset node.

This is purely a structural optimization that reduces execution overhead.
"""

from opteryx.managers.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class EmptyTableStrategy(OptimizationStrategy):  # pragma: no cover
    """
    Replace FILTER(FALSE) with empty data source.

    This strategy detects impossible filter conditions (contradictions)
    and replaces the FILTER node and its entire subtree with a FunctionDataset
    that returns an empty table with the correct schema.

    During visit(), we collect FALSE filters. In complete(), we safely rewrite
    them by replacing with empty data sources.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        During traversal, collect FILTER nodes that have FALSE conditions.
        Don't modify the graph here - just collect for later processing.
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            if self._is_false_condition(node.condition):
                # Store the node ID and the node itself for later rewriting
                context.false_filters.append((context.node_id, node))

        return context

    def should_i_run(self, plan: LogicalPlan) -> bool:
        """Only run if there are FILTER clauses with FALSE conditions in the plan."""
        for _, node in plan.nodes(True):
            if node.node_type == LogicalPlanStepType.Filter:
                if self._is_false_condition(node.condition):
                    return True
        return False

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """
        Post-process the optimized plan to replace FILTER(FALSE) with empty data source.

        We collected FALSE filters during visit(). Now safely replace them by:
        1. Finding all descendants of the filter node (NOT including the filter itself)
        2. Removing descendants one by one
        3. Replacing the filter node with an empty FunctionDataset
        """
        optimized_plan = context.optimized_plan

        # Process each FALSE filter we collected
        for filter_nid, _ in context.false_filters:
            # Collect all descendants starting from outgoing edges (NOT the filter itself)
            descendants = self._collect_descendants(filter_nid, optimized_plan)

            # Get all properties from the immediate child (Scan node) before removing it
            # This preserves the schema, columns, connector, etc.
            child_properties = {}
            if descendants:
                # The first descendant is the direct child (typically a Scan node)
                first_child_nid = descendants[0]
                first_child_node = optimized_plan[first_child_nid]
                # Copy all properties from the child node
                child_properties = dict(first_child_node.properties)

            # Remove descendants one by one, from leaf up (reverse order)
            for desc_nid in reversed(descendants):
                optimized_plan.remove_node(desc_nid, heal=True)

            # Create a Scan node with marker for empty result
            # The physical planner will see this marker and use NullReaderNode
            # Start with child properties and override connector with our marker
            empty_props = child_properties.copy()
            empty_props["node_type"] = LogicalPlanStepType.Scan
            empty_props["connector"] = (
                "__null__"  # Marker for physical planner to use NullReaderNode
            )

            empty_node = LogicalPlanNode(**empty_props)

            # Replace the filter node with the empty node (node must still exist in graph)
            optimized_plan[filter_nid] = empty_node

            self.statistics.optimization_empty_table += 1

        return optimized_plan

    def _collect_descendants(self, nid: str, plan: LogicalPlan) -> list:
        """
        Collect all descendant nodes starting from the given node.
        Returns a list of node IDs to remove.
        """
        descendants = []
        stack = list(
            plan.outgoing_edges(nid)
        )  # Start with direct children - returns (source, target, relationship)
        visited = {nid}

        while stack:
            _, current_nid, _ = stack.pop()  # Unpack (source, target, relationship)

            if current_nid not in visited:
                visited.add(current_nid)
                descendants.append(current_nid)

                # Add this node's children to the stack
                stack.extend(plan.outgoing_edges(current_nid))

        return descendants

    def _is_false_condition(self, condition) -> bool:
        """Check if a condition node is FALSE literal."""
        if condition is None:
            return False
        return condition.node_type == NodeType.LITERAL and condition.value is False
