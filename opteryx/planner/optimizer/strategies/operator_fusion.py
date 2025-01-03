# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Operator Fusion

Type: Heuristic
Goal: Chose more efficient physical implementations.

Some operators can be fused to be faster.

'Fused' opertors are when physical operations perform multiple logical operations.

Initially we fused Limit and Order operators, this allows us to use a heap sort
algorithm (basically we dicard records we know aren't going to be kept early).

Note that predicate and projection pushdowns may also fuse operators. Most commonly
we fuse the READ operator with SELECTION and PROJECTION operators, we also push into
JOINs, this is sometimes as part of the join condition, but we also push SELECTIONs
into joins.
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class OperatorFusionStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Order:
            edges = context.optimized_plan.outgoing_edges(context.node_id)
            if len(edges) == 1:
                next_node_id = edges[0][1]
                next_node = context.optimized_plan[next_node_id]
                if next_node.node_type == LogicalPlanStepType.Limit and not next_node.offset:
                    new_node = LogicalPlanNode(node_type=LogicalPlanStepType.HeapSort)
                    new_node.limit = next_node.limit
                    new_node.order_by = node.order_by
                    context.optimized_plan[next_node_id] = new_node
                    context.optimized_plan.remove_node(context.node_id, heal=True)
                    self.statistics.optimization_fuse_operators_heap_sort += 1

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
