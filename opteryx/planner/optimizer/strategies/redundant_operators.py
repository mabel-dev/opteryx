# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Remove Redundant Operators

Type: Heuristic
Goal: Remove steps which don't affect the result

This optimization runs toward the end of the set, it removes operators which
were useful during planning and optimization.

- Some projections are redundant (reselecting down to the columns which the
  providing operation has already limited down to).
- SubQuery nodes are useful for planning and optimization, but don't do
  anything during execution, we can remove them here.

Both of these operations are cheap to execute, the benefit for this
optimization isn't expected to be realized until we implement multiprocessing
and there is work associated with IPC which we are avoiding by removing
impotent steps.
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class RedundantOperationsStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        # If we're a project and the providing step has the same columns, we're
        # not doing anything so can be removed.
        if node.node_type == LogicalPlanStepType.Project:
            providers = context.pre_optimized_tree.ingoing_edges(context.node_id)
            if len(providers) == 1:
                provider_nid = providers[0][0]
                provider_node = context.pre_optimized_tree[provider_nid]
                if provider_node.node_type != LogicalPlanStepType.Subquery:
                    provider_columns = {c.schema_column.identity for c in provider_node.columns}
                    # if the columns in the project are the same as the operator before it
                    # we don't need to project
                    my_columns = {c.schema_column.identity for c in node.columns}
                    if provider_columns == my_columns:
                        # we need to ensure we keep some of the context if not the step
                        source_node_alias = context.optimized_plan[context.node_id].alias
                        if provider_node.all_relations:
                            provider_node.all_relations.add(source_node_alias)
                        else:
                            provider_node.all_relations = {source_node_alias}
                        context.optimized_plan.add_node(provider_nid, provider_node)
                        # remove the node
                        context.optimized_plan.remove_node(context.node_id, heal=True)
                        self.statistics.optimization_remove_redundant_operators_project += 1

        # Subqueries are useful for planning but not needed for execution
        # We need to ensure the alias of the subquery is pushed
        if node.node_type == LogicalPlanStepType.Subquery:
            alias = node.alias
            nid = context.optimized_plan.ingoing_edges(context.node_id)[0][0]
            updated_node = context.optimized_plan[nid]
            # if we have multiple layers of subqueries, ignore everything other than the outermost
            while updated_node.node_type == LogicalPlanStepType.Subquery:
                nid = context.optimized_plan.ingoing_edges(nid)[0][0]
                updated_node = context.optimized_plan[nid]
            updated_node.alias = alias
            if updated_node.all_relations:
                updated_node.all_relations.add(alias)
            else:
                updated_node.all_relations = {alias}
            context.optimized_plan.add_node(nid, updated_node)
            context.optimized_plan.remove_node(context.node_id, heal=True)
            self.statistics.optimization_remove_redundant_operators_subquery += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
