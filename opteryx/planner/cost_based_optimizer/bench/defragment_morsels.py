# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from orso.tools import random_string

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import HeuristicOptimizerContext
from .optimization_strategy import OptimizationStrategy


class DefragmentMorselsStrategy(OptimizationStrategy):  # pragma: no cover
    def visit(
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> HeuristicOptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type in (LogicalPlanStepType.Join,):
            for node, _, _ in context.optimized_plan.ingoing_edges(context.node_id):
                defrag = LogicalPlanNode(node_type=LogicalPlanStepType.Defragment)
                context.optimized_plan.insert_node_after(random_string(), defrag, node)
        return context

    def complete(self, plan: LogicalPlan, context: HeuristicOptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
