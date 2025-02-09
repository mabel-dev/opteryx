# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate rewriter

Type: Heuristic
Goal: Chose more efficient predicate evaluations

We rewrite some conditions to a more optimal form; for example if doing a
LIKE comparison and the pattern contains no wildcards, we rewrite to be an
equals check.

Rewrite to a form which is just faster, even if it can't be pushed
"""

import re
from typing import Callable
from typing import Dict

from orso.types import OrsoTypes

from opteryx.managers.expression import ExpressionColumn
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import format_expression
from opteryx.models import Node
from opteryx.models import QueryStatistics
from opteryx.planner.binder.operator_map import determine_type
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

IN_REWRITES = {"InList": "Eq", "NotInList": "NotEq"}
LIKE_REWRITES = {"Like": "Eq", "NotLike": "NotEq"}
LITERALS_TO_THE_RIGHT = {"Plus": "Minus", "Minus": "Plus"}
INSTR_REWRITES = {
    "Like": "InStr",
    "NotLike": "NotInStr",
    "ILike": "IInStr",
    "NotILike": "NotIInStr",
}


def rewrite_in_to_eq(predicate):
    """
    Rewrite IN conditions with a single value to equality conditions.

    If the IN condition contains only one value, it is equivalent to an equality check.
    This optimization replaces the IN condition with a faster equality check.
    """
    predicate.value = IN_REWRITES[predicate.value]
    predicate.right.value = predicate.right.value.pop()
    predicate.right.type = predicate.right.sub_type or OrsoTypes.VARCHAR
    predicate.right.sub_type = None
    return predicate


def reorder_interval_calc(predicate):
    """
    rewrite:
        end - start > interval => start + interval > end

    This is because comparing a Date with a Date is faster than
    comparing in Interval with an Interval.
    """
    date_start = predicate.left.right
    date_end = predicate.left.left
    interval = predicate.right

    # Check if the operation is date - date
    if predicate.left.value == "Minus":
        # Create a new binary operator node for date + interval
        new_binary_op = Node(
            node_type=NodeType.BINARY_OPERATOR,
            value="Plus",
            left=date_start,
            right=interval,
        )
        binary_op_column_name = format_expression(new_binary_op, True)
        new_binary_op.schema_column = ExpressionColumn(
            name=binary_op_column_name, type=OrsoTypes.TIMESTAMP
        )

        # Create a new comparison operator node for date > date
        predicate.node_type = NodeType.COMPARISON_OPERATOR
        predicate.right = new_binary_op
        predicate.left = date_end

        predicate_column_name = format_expression(predicate, True)
        predicate.schema_column = ExpressionColumn(
            name=predicate_column_name, type=OrsoTypes.BOOLEAN
        )

        return predicate


# Define dispatcher conditions and actions
dispatcher: Dict[str, Callable] = {
    "rewrite_in_to_eq": rewrite_in_to_eq,
    "reorder_interval_calc": reorder_interval_calc,
}


# Dispatcher conditions
def _rewrite_predicate(predicate, statistics: QueryStatistics):
    if predicate.node_type not in {NodeType.BINARY_OPERATOR, NodeType.COMPARISON_OPERATOR}:
        # after rewrites, some filters aren't actually predicates
        return predicate

    if predicate.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
        predicate.left = _rewrite_predicate(predicate.left, statistics)
        predicate.right = _rewrite_predicate(predicate.right, statistics)

    if predicate.right.type == OrsoTypes.VARCHAR:
        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if "%%" in predicate.right.value:
                statistics.optimization_predicate_rewriter_remove_adjacent_wildcards += 1
                predicate.right.value = re.sub(r"%+", "%", predicate.right.value)

        if predicate.value in LIKE_REWRITES:
            if "%" not in predicate.right.value and "_" not in predicate.right.value:
                statistics.optimization_predicate_rewriter_remove_redundant_like += 1
                predicate.value = LIKE_REWRITES[predicate.value]

        if predicate.value in INSTR_REWRITES:
            if (
                "_" not in predicate.right.value
                and predicate.right.value.endswith("%")
                and predicate.right.value.startswith("%")
            ):
                statistics.optimization_predicate_rewriter_replace_like_with_in_string += 1
                predicate.right.value = predicate.right.value[1:-1]
                predicate.value = INSTR_REWRITES[predicate.value]

    if predicate.right.type == OrsoTypes.BLOB:
        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if b"%%" in predicate.right.value:
                statistics.optimization_predicate_rewriter_remove_adjacent_wildcards += 1
                predicate.right.value = re.sub(b"%+", b"%", predicate.right.value)

        if predicate.value in LIKE_REWRITES:
            if b"%" not in predicate.right.value and b"_" not in predicate.right.value:
                statistics.optimization_predicate_rewriter_remove_redundant_like += 1
                predicate.value = LIKE_REWRITES[predicate.value]

        if predicate.value in INSTR_REWRITES:
            if (
                b"_" not in predicate.right.value
                and predicate.right.value.endswith(b"%")
                and predicate.right.value.startswith(b"%")
            ):
                statistics.optimization_predicate_rewriter_replace_like_with_in_string += 1
                predicate.right.value = predicate.right.value[1:-1]
                predicate.value = INSTR_REWRITES[predicate.value]

    if predicate.value == "AnyOpEq":
        if predicate.right.node_type == NodeType.LITERAL:
            statistics.optimization_predicate_rewriter_any_to_inlist += 1
            predicate.value = "InList"

    if predicate.value in IN_REWRITES:
        if predicate.right.node_type == NodeType.LITERAL and len(predicate.right.value) == 1:
            statistics.optimization_predicate_rewriter_in_to_equals += 1
            return dispatcher["rewrite_in_to_eq"](predicate)

    if (
        predicate.node_type == NodeType.COMPARISON_OPERATOR
        and predicate.left.node_type == NodeType.BINARY_OPERATOR
    ):
        if (
            determine_type(predicate.left) == OrsoTypes.INTERVAL
            and determine_type(predicate.right) == OrsoTypes.INTERVAL
        ):
            statistics.optimization_predicate_rewriter_date_ += 1
            predicate = dispatcher["reorder_interval_calc"](predicate)

    return predicate


def _rewrite_function(function, statistics: QueryStatistics):
    """
    Rewrite CASE WHEN x IS NULL THEN y ELSE x END to IFNULL(x, y)
    """
    if function.node_type == NodeType.FUNCTION and function.value == "CASE":
        if len(function.parameters) == 2 and function.parameters[0].parameters[0].value == "IsNull":
            compare_column = function.parameters[0].parameters[0].centre
            target_column = function.parameters[1].parameters[1]
            value_if_null = function.parameters[1].parameters[0]

            if compare_column.schema_column.identity == target_column.schema_column.identity:
                statistics.optimization_predicate_rewriter_case_to_ifnull += 1
                function.value = "IFNULL"
                function.parameters = [compare_column, value_if_null]
                return function
    return function


class PredicateRewriteStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            node.condition = _rewrite_predicate(node.condition, self.statistics)
            context.optimized_plan[context.node_id] = node

        if node.node_type == LogicalPlanStepType.Project:
            new_columns = []
            for column in node.columns:
                new_column = _rewrite_predicate(column, self.statistics)
                new_column = _rewrite_function(new_column, self.statistics)
                new_columns.append(new_column)
            node.columns = new_columns
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
