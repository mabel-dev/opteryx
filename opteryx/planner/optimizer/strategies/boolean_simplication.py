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

        Demorgan's Laws (Binary)
            not (A or B) = (not A) and (not B)

        De Morgan's Laws (N-ary Extension)
            not (A or B or C ...) = (not A) and (not B) and (not C) ...
            Creates multiple AND conditions for better predicate pushdown

        De Morgan's for IN Lists
            not (col IN (a, b, c)) = col != a and col != b and col != c
            Expands IN predicates to multiple AND-ed conditions

        Negative Reduction:
            not (A = B) = A != B
            not (A != B) = A = B
            not (not (A)) = A

        Constant Folding:
            A AND TRUE => A
            A AND FALSE => FALSE
            A OR TRUE => TRUE
            A OR FALSE => A

        AND Chain Flattening:
            ((A AND B) AND C) => (A AND (B AND C))

        Redundant Condition Removal:
            A AND A => A

    These simplifications help prepare conditions for predicate pushdown by creating
    simple chains of AND conditions that can be more easily pushed down through the
    query plan.
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


def _is_literal_true(node: LogicalPlanNode) -> bool:
    """Check if a node is a literal TRUE value."""
    if node is None:
        return False
    if node.node_type == NodeType.LITERAL:
        return node.value is True
    return False


def _is_literal_false(node: LogicalPlanNode) -> bool:
    """Check if a node is a literal FALSE value."""
    if node is None:
        return False
    if node.node_type == NodeType.LITERAL:
        return node.value is False
    return False


def _flatten_and_chain(node: LogicalPlanNode, statistics: QueryStatistics) -> list:
    """
    Flatten nested AND chains into a list of conditions.
    e.g., ((A AND B) AND C) becomes [A, B, C]
    """
    if node is None:
        return []
    if node.node_type != NodeType.AND:
        return [node]

    left_conditions = _flatten_and_chain(node.left, statistics)
    right_conditions = _flatten_and_chain(node.right, statistics)
    return left_conditions + right_conditions


def _rebuild_and_chain(conditions: list) -> LogicalPlanNode:
    """
    Rebuild AND chain from a list of conditions.
    [A, B, C] becomes ((A AND B) AND C)
    """
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]

    result = conditions[0]
    for condition in conditions[1:]:
        result = Node(NodeType.AND, left=result, right=condition)
    return result


def _flatten_or_chain(node: LogicalPlanNode) -> list:
    """
    Flatten nested OR chains into a list of conditions.
    e.g., ((A OR B) OR C) becomes [A, B, C]
    """
    if node is None:
        return []
    if node.node_type != NodeType.OR:
        return [node]

    left_conditions = _flatten_or_chain(node.left)
    right_conditions = _flatten_or_chain(node.right)
    return left_conditions + right_conditions


def _rebuild_or_chain(conditions: list) -> LogicalPlanNode:
    """
    Rebuild OR chain from a list of conditions.
    [A, B, C] becomes ((A OR B) OR C)
    """
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]

    result = conditions[0]
    for condition in conditions[1:]:
        result = Node(NodeType.OR, left=result, right=condition)
    return result


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

        # De Morgan's n-ary: NOT (A OR B OR C ...) => (NOT A) AND (NOT B) AND (NOT C) ...
        # This creates more AND conditions that can be pushed down
        if centre_node.node_type == NodeType.OR:
            # Flatten the OR chain to handle all conditions
            or_conditions = _flatten_or_chain(centre_node)

            # If we have 2+ conditions, apply De Morgan's to all
            if len(or_conditions) >= 2:
                # Create NOT of each condition
                not_conditions = [
                    Node(NodeType.NOT, centre=condition) for condition in or_conditions
                ]

                # Rebuild as AND chain (highly pushable!)
                result = not_conditions[0]
                for condition in not_conditions[1:]:
                    result = Node(NodeType.AND, left=result, right=condition)

                # Track statistic based on chain length
                if len(or_conditions) > 2:
                    statistics.optimization_boolean_rewrite_demorgan_nary += 1
                else:
                    statistics.optimization_boolean_rewrite_demorgan += 1

                return update_expression_tree(result, statistics)

        # NOT(A = B) => A != B
        if centre_node.value in INVERSIONS:
            centre_node.value = INVERSIONS[centre_node.value]
            statistics.optimization_boolean_rewrite_inversion += 1
            return update_expression_tree(centre_node, statistics)

        # De Morgan's for NOT IN: NOT(col IN (a,b,c)) => col != a AND col != b AND col != c
        # This expands a single predicate into multiple AND-ed conditions for better pushdown
        if centre_node.node_type == NodeType.COMPARISON_OPERATOR and centre_node.value == "InList":
            in_list_values = centre_node.right.value  # Should be tuple/list

            # Only expand if we have 2+ values (1 value is better as NOT EQ)
            if isinstance(in_list_values, (tuple, list)) and len(in_list_values) > 1:
                col = centre_node.left

                # Create != (NOT EQ) predicates for each value
                ne_predicates = []
                for value in in_list_values:
                    ne_pred = Node(
                        NodeType.COMPARISON_OPERATOR,
                        value="NotEq",
                        left=col,
                        right=Node(NodeType.LITERAL, value=value),
                    )
                    ne_predicates.append(ne_pred)

                # Rebuild as AND chain
                result = ne_predicates[0]
                for pred in ne_predicates[1:]:
                    result = Node(NodeType.AND, left=result, right=pred)

                statistics.optimization_boolean_rewrite_demorgan_in_expansion += 1
                return update_expression_tree(result, statistics)

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

    # Additional AND simplifications for predicate pushdown
    if node.node_type == NodeType.AND:
        # A AND TRUE => A
        if _is_literal_true(node.right):
            statistics.optimization_boolean_rewrite_and_true += 1
            return node.left
        if _is_literal_true(node.left):
            statistics.optimization_boolean_rewrite_and_true += 1
            return node.right

        # A AND FALSE => FALSE
        if _is_literal_false(node.right):
            statistics.optimization_boolean_rewrite_and_false += 1
            return node.right
        if _is_literal_false(node.left):
            statistics.optimization_boolean_rewrite_and_false += 1
            return node.left

        # Flatten nested AND chains to prepare for predicate pushdown
        # Only flatten chains with more than 2 conditions to enable better pushdown
        # ((A AND B) AND C) => (A AND (B AND C))
        conditions = _flatten_and_chain(node, statistics)

        # Only proceed with flattening if we have more than 2 conditions
        if len(conditions) > 2:
            # Remove duplicate conditions from the chain
            # A AND A => A
            unique_conditions = []
            for condition in conditions:
                is_duplicate = False
                for existing in unique_conditions:
                    # Simple check: compare node structure (UUIDs should be same for duplicates)
                    if (
                        hasattr(condition, "uuid")
                        and hasattr(existing, "uuid")
                        and condition.uuid == existing.uuid
                    ):
                        statistics.optimization_boolean_rewrite_and_redundant += 1
                        is_duplicate = True
                        break
                if not is_duplicate:
                    unique_conditions.append(condition)

            # Rebuild the chain for better pushdown
            # This creates a left-associative chain that's easier to traverse
            if len(unique_conditions) < len(conditions) or len(unique_conditions) > 2:
                statistics.optimization_boolean_rewrite_and_flatten += 1
                return _rebuild_and_chain(unique_conditions)

    return node
