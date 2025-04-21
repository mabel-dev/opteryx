# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Demorgan's Laws

Type: Heuristic
Goal: Preposition for following actions
"""

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.models import QueryStatistics
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Operations safe to invert.
HALF_INVERSIONS: dict = {
    "Eq": "NotEq",
    "Gt": "LtEq",
    "GtEq": "Lt",
    "Like": "NotLike",
    "ILike": "NotILike",
    "RLike": "NotRLike",
    "InStr": "NotInStr",
    "IInStr": "NotIInStr",
    # Any to All conversions (De Morgan's laws)
    "AnyOpEq": "AllOpNotEq",  # NOT(ANY x = y) → ALL x != y
    "AnyOpGtEq": "AllOpLt",  # NOT(ANY x >= y) → ALL x < y
}

INVERSIONS = {**HALF_INVERSIONS, **{v: k for k, v in HALF_INVERSIONS.items()}}


class BooleanSimplificationStrategy(OptimizationStrategy):  # pragma: no cover
    """
    This action aims to rewrite and simplify expressions.

    This has two purposes:
     1) Reduce the work to evaluate expressions by removing steps
     2) Express conditions in ways that other strategies can act on, e.g. pushing
        predicates.

    The core of this action takes advantage of the following:

        Demorgan's Law
            not (A or B) = (not A) and (not B)

        Negative Reduction:
            not (A = B) = A != B
            not (A != B) = A = B
            not (not (A)) = A

    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            # do the work
            node.condition = update_expression_tree(node.condition, self.statistics)
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are FILTER clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Filter,))
        return len(candidates) > 0


def update_expression_tree(node: LogicalPlanNode, statistics: QueryStatistics):
    # break out of nests
    if node.node_type == NodeType.NESTED:
        return update_expression_tree(node.centre, statistics)

    # handle rules relating to NOTs
    if node.node_type == NodeType.NOT:
        centre_node = node.centre

        # break out of nesting
        if centre_node.node_type == NodeType.NESTED:
            centre_node = centre_node.centre

        # NOT (A OR B) => (NOT A) AND (NOT B)
        if centre_node.node_type == NodeType.OR:
            # rewrite to (not A) and (not B)
            a_side = Node(NodeType.NOT, centre=centre_node.left)
            b_side = Node(NodeType.NOT, centre=centre_node.right)
            statistics.optimization_boolean_rewrite_demorgan += 1
            return update_expression_tree(Node(NodeType.AND, left=a_side, right=b_side), statistics)

        # NOT(A = B) => A != B
        if centre_node.value in INVERSIONS:
            centre_node.value = INVERSIONS[centre_node.value]
            statistics.optimization_boolean_rewrite_inversion += 1
            return update_expression_tree(centre_node, statistics)

        # NOT(NOT(A)) => A
        if centre_node.node_type == NodeType.NOT:
            statistics.optimization_boolean_rewrite_double_not += 1
            return update_expression_tree(centre_node.centre, statistics)

    # traverse the expression tree
    node.left = None if node.left is None else update_expression_tree(node.left, statistics)
    node.centre = None if node.centre is None else update_expression_tree(node.centre, statistics)
    node.right = None if node.right is None else update_expression_tree(node.right, statistics)
    if node.parameters:
        node.parameters = [
            parameter
            if not isinstance(parameter, Node)
            else update_expression_tree(parameter, statistics)
            for parameter in node.parameters
        ]

    return node
