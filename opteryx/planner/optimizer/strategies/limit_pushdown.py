# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Limit Pushdown

Type: Heuristic
Goal: Reduce Rows

We try to push the limit to the other side of PROJECTS. This optimization
pushes LIMIT and HEAPSORT operations before projections when the limiting/ordering
columns are not created by the projection, reducing the number of rows that need
to be processed by expensive projection calculations.
"""

from opteryx.connectors.capabilities import LimitPushable
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class LimitPushdownStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        # Collect LIMIT nodes for potential pushdown
        if node.node_type == LogicalPlanStepType.Limit:
            if node.offset is not None:
                # we can't push down limits with offset
                return context
            node.nid = context.node_id
            context.collected_limits.append(node)
            return context

        # Collect HEAPSORT nodes for potential pushdown
        if node.node_type == LogicalPlanStepType.HeapSort:
            node.nid = context.node_id
            context.collected_limits.append(node)
            return context

        # Handle Project nodes - try to push limits before expensive projections
        if node.node_type == LogicalPlanStepType.Project:
            pushable_limits = []
            for limit_node in context.collected_limits:
                if self._can_push_before_projection(limit_node, node):
                    # This limit/heapsort can be pushed before the projection
                    pushable_limits.append(limit_node)

            # Push the limits that can be pushed
            for limit_node in pushable_limits:
                self.statistics.optimization_limit_pushdown += 1
                context.optimized_plan.remove_node(limit_node.nid, heal=True)
                context.optimized_plan.insert_node_after(
                    limit_node.nid, limit_node, context.node_id
                )
                limit_node.columns = []
                context.collected_limits.remove(limit_node)

        # Try to push limits into scans that support it
        if (
            node.node_type == LogicalPlanStepType.Scan
            and LimitPushable in node.connector.__class__.mro()
        ):
            for limit_node in context.collected_limits:
                if node.relation in limit_node.all_relations:
                    self.statistics.optimization_limit_pushdown += 1
                    context.optimized_plan.remove_node(limit_node.nid, heal=True)
                    node.limit = limit_node.limit
                    context.optimized_plan[context.node_id] = node
        elif node.node_type in (
            LogicalPlanStepType.Aggregate,
            LogicalPlanStepType.AggregateAndGroup,
            LogicalPlanStepType.Distinct,
            LogicalPlanStepType.Filter,
            LogicalPlanStepType.Join,
            LogicalPlanStepType.Order,
            LogicalPlanStepType.Union,
            LogicalPlanStepType.Scan,
        ):
            # we don't push past here
            for limit_node in context.collected_limits:
                self.statistics.optimization_limit_pushdown += 1
                context.optimized_plan.remove_node(limit_node.nid, heal=True)
                context.optimized_plan.insert_node_after(
                    limit_node.nid, limit_node, context.node_id
                )
                limit_node.columns = []
            context.collected_limits.clear()

        return context

    def _can_push_before_projection(
        self, limit_node: LogicalPlanNode, project_node: LogicalPlanNode
    ) -> bool:
        """
        Determine if a LIMIT or HEAPSORT node can be pushed before a projection.

        A LIMIT can always be pushed before a projection.
        A HEAPSORT can be pushed if all ORDER BY columns existed before the projection
        (i.e., they are not computed/created by the projection).

        Args:
            limit_node: The LIMIT or HEAPSORT node to potentially push
            project_node: The PROJECT node to potentially push past

        Returns:
            True if the limit/heapsort can be pushed before the projection
        """
        # LIMIT nodes can always be pushed before projections
        if limit_node.node_type == LogicalPlanStepType.Limit:
            return True

        # For HEAPSORT, check if ORDER BY columns are from before the projection
        if limit_node.node_type == LogicalPlanStepType.HeapSort:
            # Get the columns that existed before this projection
            # (from pre_update_columns set by projection_pushdown)
            columns_before_projection = getattr(project_node, "pre_update_columns", None)

            # If we can't determine what columns existed before, don't push
            if not columns_before_projection:
                return False

            # Check if all ORDER BY columns existed before the projection
            order_by = getattr(limit_node, "order_by", [])
            for order_col, _ in order_by:
                # Get the identity of the column being ordered by
                if hasattr(order_col, "schema_column") and order_col.schema_column:
                    col_identity = order_col.schema_column.identity
                    if col_identity not in columns_before_projection:
                        # This column is created by the projection, can't push
                        return False
                else:
                    # Can't determine column identity, don't push to be safe
                    return False

            # All ORDER BY columns existed before the projection
            return True

        return False

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT or HEAPSORT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(
            plan, (LogicalPlanStepType.Limit, LogicalPlanStepType.HeapSort)
        )
        return len(candidates) > 0
