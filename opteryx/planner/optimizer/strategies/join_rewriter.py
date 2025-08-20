# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Join Rewriter

Type: Heuristic
Goal: Faster Joins

Rewrite joins to more efficient versions based on heuristics.

We collect references to LEFT JOINs and the right relation as we traverse the plan.
"""

from opteryx.managers.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

LEFT_REWRITABLE_CONDITIONS = ("Eq", "Gt", "Lt", "Gte", "Lte")


class JoinRewriteStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            context.collected_predicates.append(node)

        if node.node_type == LogicalPlanStepType.Join and node.type == "left outer":
            context.collected_joins.append(node)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        if context.collected_joins and context.collected_predicates:
            for join in context.collected_joins:
                left_side = join.left_relation_names
                can_rewrite = None
                for predicate in context.collected_predicates:
                    if predicate.condition.value in LEFT_REWRITABLE_CONDITIONS and set(
                        left_side
                    ).intersection(predicate.all_relations):
                        if (
                            predicate.condition.left.node_type == NodeType.IDENTIFIER
                            and predicate.condition.right.node_type == NodeType.IDENTIFIER
                        ):
                            import warnings

                            warnings.warn("LEFT OUTER JOIN -> INNER JOIN rewriter not implemented")
                    if predicate.condition.value == "IsNull" and set(left_side).intersection(
                        predicate.all_relations
                    ):
                        import warnings

                        warnings.warn("LEFT OUTER JOIN -> SEMI JOIN rewriter not implemented")
                    if predicate.condition.value == "IsNotNull" and set(left_side).intersection(
                        predicate.all_relations
                    ):
                        import warnings

                        warnings.warn("LEFT OUTER JOIN -> ANTI JOIN rewriter not implemented")
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Join,))
        return len(candidates) > 0
