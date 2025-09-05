# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate Ordering

Type: Cost
Goal: Faster Execution

We combine adjacent predicates into chains of ANDed conditions in a single
filtering step. We use cost estimates based on the data type to determine
a good order to execute the filters.

NOTE: This currently doesn't account for a few very important things which
means this may create an order that is slower than the order that would have
been run if this strategy didn't run.

- The selectivity of the filter
- How to handle complex sub conditions or ORed conditions
"""

from orso.tools import random_string

# pragma: no cover
from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Approximate of the time in seconds (3dp) to compare 1 million records
# None indicates no comparison is possible
# These are the core comparisons, Eq, NotEq, Gt, GtEq, Lt, LtEq
BASIC_COMPARISON_COSTS = {
    OrsoTypes.ARRAY: 10.00,  # expensive
    OrsoTypes.BLOB: 0.06,  # varies based on length, this is 50 bytes
    OrsoTypes.BOOLEAN: 0.004,
    OrsoTypes.DATE: 0.01,
    OrsoTypes.DECIMAL: 2.35,
    OrsoTypes.DOUBLE: 0.003,
    OrsoTypes.INTEGER: 0.002,
    OrsoTypes.INTERVAL: 10.00,  # expensive
    OrsoTypes.STRUCT: 10.00,  # expensive
    OrsoTypes.TIMESTAMP: 0.009,
    OrsoTypes.TIME: 10.00,  # expensive
    OrsoTypes.VARCHAR: 0.3,  # varies based on length, this is 50 chars
    OrsoTypes.NULL: 10.00,  # for completeness
    OrsoTypes._MISSING_TYPE: 10.00,  # for completeness
    0: 10.00,  # for completeness
}


def order_predicates(predicates: list, statistics) -> list:
    """
    This is a fairly naive cost-based predicate ordering routine.

    We have the time it takes for each comparison based on the datatype
    of the values - we order the 'cheapest' (fastest) comparisons first
    """
    cost_estimates = [
        (
            10.00
            if c.condition.left is None
            else BASIC_COMPARISON_COSTS[c.condition.left.schema_column.type],
            i,
        )
        for i, c in enumerate(predicates)
    ]
    # Lowest execution times to the front
    cost_estimates.sort(key=lambda x: x[0], reverse=False)
    ordered_predicates = [predicates[c[1]] for c in cost_estimates]
    # Did this optimization change the ordering?
    for i, e in enumerate(cost_estimates):
        if i > 0 and cost_estimates[i - 1][1] > e[1]:
            statistics.optimization_cost_based_predicate_ordering += 1
            break
    return ordered_predicates


class PredicateOrderingStrategy(OptimizationStrategy):
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
            context.collected_predicates = order_predicates(
                context.collected_predicates, self.statistics
            )
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
