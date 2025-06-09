# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate Flattening

Type: Heuristic
Goal: Fewer Operations

If we have chains of filters, we can flatten them into a ANDed single filter.

A later cost-based strategy can then order these filters to be more efficient.
"""

from orso.tools import random_string

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan


class PredicateFlatteningStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            node.nid = context.node_id
            context.collected_predicates.append(node)
            return context

        if node.node_type != LogicalPlanStepType.Filter and context.collected_predicates:
            if len(context.collected_predicates) == 1:
                context.collected_predicates = []
                return context

            new_node = LogicalPlanNode(LogicalPlanStepType.Filter)
            new_node.condition = Node(node_type=NodeType.DNF)
            new_node.condition.parameters = [c.condition for c in context.collected_predicates]
            new_node.columns = []
            new_node.relations = set()
            new_node.all_relations = set()

            for predicate in context.collected_predicates:
                new_node.columns.extend(predicate.columns)
                new_node.relations.update(predicate.relations)
                new_node.all_relations.update(predicate.all_relations)
                self.statistics.optimization_flatten_filters += 1
                context.optimized_plan.remove_node(predicate.nid, heal=True)

            context.optimized_plan.insert_node_after(random_string(), new_node, context.node_id)
            context.collected_predicates.clear()

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are LIMIT clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Filter,))
        return len(candidates) > 0
