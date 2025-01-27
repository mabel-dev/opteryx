# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Limit Pushdown

Type: Heuristic
Goal: Reduce Rows

We try to push the limit to the other side of PROJECTS
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

        if node.node_type == LogicalPlanStepType.Limit:
            node.nid = context.node_id
            context.collected_limits.append(node)
            return context

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

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Limit,))
        return len(candidates) > 0
