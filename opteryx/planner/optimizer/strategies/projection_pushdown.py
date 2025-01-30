# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Projection Pushdown

Type: Heuristic
Goal: Limit columns which need to be moved around

We bind from the the scans, exposing the available columns to each operator
as we make our way to the top of the plan (usually the SELECT). The projection
pushdown is done as part of the optimizers, but isn't quite like the other
optimizations; this is collecting used column information as it goes from the
top of the plan down to the selects. The other optimizations tend to move or
remove operations, or update what a step does, this is just collecting and
updating the used columns.
"""

from typing import Set

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class ProjectionPushdownStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Optimize the given node by pushing projections down in the plan.

        Args:
            node: The current node in the logical plan to be optimized.
            context: The context carrying the state and information for optimization.

        Returns:
            The updated context, including updated node information.
        """
        node.pre_update_columns = set(context.collected_identities)

        # If we're at a union, it changes what we think we know about the columns.
        if node.node_type == LogicalPlanStepType.Union:
            context.seen_unions += 1

        # If we're at the something other than the top project (e.g. in a subquery)
        # in a plan we may be able to remove some columns (and potentially some
        # evaluations) if the columns aren't referenced in the outer query.
        if node.node_type == LogicalPlanStepType.Project:
            if context.seen_unions == 0 and context.seen_projections > 0:
                node.columns = [
                    n for n in node.columns if n.schema_column.identity in node.pre_update_columns
                ]
            if context.seen_unions == 0:
                context.seen_projections += 1

        # Subqueries act like all columns are referenced
        if node.node_type != LogicalPlanStepType.Subquery:
            if node.columns:  # Assumes node.columns is an iterable or None
                collected_columns = self.collect_columns(node)
                context.collected_identities.update(collected_columns)

        if (
            node.node_type
            in (
                LogicalPlanStepType.Scan,
                LogicalPlanStepType.Subquery,
                LogicalPlanStepType.Union,
            )
            and hasattr(node, "schema")
            and hasattr(node.schema, "columns")
        ):
            # Push all of the projections
            node_columns = [
                LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=col.name,
                    source=col.origin[0],
                    schema_column=col,
                )
                for col in node.schema.columns
                if col.identity in context.collected_identities
            ]
            # Update the node with the pushed columns
            node.columns = node_columns

        context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))
        if context.parent_nid:
            context.optimized_plan.add_edge(context.node_id, context.parent_nid)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def collect_columns(self, node: LogicalPlanNode) -> Set[str]:
        """
        Collect and return the set of column identities from the given node.

        Args:
            node: The node from which to collect column identities.

        Returns:
            A set of column identities.
        """
        identities = set()
        for column in node.columns or []:  # Ensuring that node.columns is iterable
            if column.node_type == NodeType.IDENTIFIER and column.schema_column:
                identities.add(column.schema_column.identity)
            else:
                identities.update(
                    col.schema_column.identity
                    for col in get_all_nodes_of_type(column, (NodeType.IDENTIFIER,))
                    if col.schema_column
                )

        return identities
