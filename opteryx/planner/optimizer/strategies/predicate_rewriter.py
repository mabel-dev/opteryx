# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate rewriter

Type: Heuristic
Goal: Chose more efficient predicate evaluations

We rewrite conditions to a more optimal form based on two objectives:
1) the execution of the condition is faster
2) the condition is more likely to be able to be pushed to the storage layer (where its faster)

Rewrites Implemented:

x IN (single_value)                         → x = single_value
x NOT IN (single_value)                     → x != single_value
x LIKE 'pattern'                            → x = 'pattern' (when no wildcards)
x NOT LIKE 'pattern'                        → x != 'pattern' (when no wildcards)
x LIKE '%pattern%'                          → x INSTR 'pattern' (for contains without underscores)
x NOT LIKE '%pattern%'                      → x NOT INSTR 'pattern' (for contains without underscores)
x ILIKE '%pattern%'                         → x IINSTR 'pattern' (case-insensitive version)
x NOT ILIKE '%pattern%'                     → x NOT IINSTR 'pattern' (case-insensitive version)
x LIKE '%%%pattern%%'                       → x LIKE '%pattern%' (removing adjacent wildcards)
x ANY_OP = value                            → x IN (value) (when right side is a literal)
end - start > interval                      → start + interval < end (for date comparisons)
CASE WHEN x IS NULL THEN y ELSE x END       → IFNULL(x, y)
CASE WHEN x THEN y ELSE z END               → IIF(x, y, z)
COALESCE(x, y)                              → IFNULL(x, y) (when only two parameters)
SUBSTRING(x, 1, n)                          → LEFT(x, n) (when starting at position 1)
x LIKE 'pattern1%' OR x LIKE '%pattern2'    → x REGEX '^pattern1.*|.*pattern2$' (for ORed LIKE conditions)
CONCAT(x, y, z)                             → x || y || z (CONCAT to operators)
CONCAT_WS(x, y, z)                          → y || x || z (CONCAT_WS to operators)
x = 'a' OR x = 'b' OR x = 'c'               → x IN ('a', 'b', 'c') (for ORed Equals conditions)
a = ANY(z) OR b = ANY(z) OR c = ANY(z)      → (a, b, c) @> z
ENDS_WITH(x, pattern)                       → x LIKE '%pattern'
STARTS_WITH(x, pattern)                     → x LIKE 'pattern%'
"""

import re
from typing import Callable
from typing import Dict

from orso.schema import ConstantColumn
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
from opteryx.utils.sql import sql_like_to_regex

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# fmt: off
IN_REWRITES = {"InList": "Eq", "NotInList": "NotEq"}
LIKE_REWRITES = {"Like": "Eq", "NotLike": "NotEq"}
LITERALS_TO_THE_RIGHT = {"Plus": "Minus", "Minus": "Plus"}
INSTR_REWRITES = {"Like": "InStr", "NotLike": "NotInStr", "ILike": "IInStr", "NotILike": "NotIInStr"}
# fmt: on


def rewrite_in_to_eq(predicate):
    """
    Rewrite IN conditions with a single value to equality conditions.

    If the IN condition contains only one value, it is equivalent to an equality check.
    This optimization replaces the IN condition with a faster equality check.
    """
    predicate.value = IN_REWRITES[predicate.value]
    predicate.right.value = tuple(predicate.right.value)[0]
    predicate.right.type = predicate.right.element_type or OrsoTypes.VARCHAR
    predicate.right.element_type = None
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


def rewrite_ored_like_to_regex(predicate, statistics):
    """
    Rewrite multiple OR'ed LIKE conditions on the same column to a single regex pattern.

    Example:
    col LIKE 'pattern1%' OR col LIKE '%pattern2' OR col LIKE '%pattern3%'
    -->
    col REGEX '^pattern1.*|.*pattern2$|.*pattern3.*$'

    This optimization reduces multiple string pattern checks to a single regex evaluation.
    """
    # Collect LIKE conditions that can be combined
    like_conditions = {}

    def collect_likes(node, likes_dict):
        # Base case: LIKE/ILIKE condition
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value in {"Like", "ILike"}:
            # Only proceed if the right side is a literal
            if node.right.node_type == NodeType.LITERAL:
                # Get column identifier for grouping
                col_id = None
                if node.left.node_type == NodeType.IDENTIFIER:
                    col_id = node.left.schema_column.identity

                if col_id:
                    is_case_sensitive = node.value == "Like"
                    if col_id not in likes_dict:
                        likes_dict[col_id] = {
                            "patterns": [],
                            "nodes": [],
                        }

                    likes_dict[col_id]["patterns"].append(
                        sql_like_to_regex(
                            node.right.value, full_match=False, case_sensitive=is_case_sensitive
                        )
                    )
                    likes_dict[col_id]["nodes"].append(node)
            return

        # Recursive cases
        if node.node_type == NodeType.OR:
            collect_likes(node.left, likes_dict)
            collect_likes(node.right, likes_dict)

    collect_likes(predicate, like_conditions)

    for col_id, like_data in like_conditions.items():
        if len(like_data["patterns"]) > 1:
            statistics.optimization_predicate_rewriter_like_to_regex += 1
            # Create a new regex pattern
            regex_pattern = "|".join(pattern for pattern in like_data["patterns"])
            new_node = like_data["nodes"][0]
            new_node.value = "RLike"
            new_node.right.value = regex_pattern
            for node in like_data["nodes"][1:]:
                node.value = False
                node.node_type = NodeType.LITERAL
                node.type = OrsoTypes.BOOLEAN

    return predicate


def rewrite_ored_any_eq_to_contains(predicate, statistics):
    """
    Rewrite multiple OR'ed ANYOPEQ conditions on the same column to a single @> condition.

    Example:
    'a' = ANY(z) OR 'b' = ANY(z) OR 'c' = ANY(z)
    -->
    ('a', 'b', 'c') @> z

    This rewrite reduces many repeated ANY checks to a single containment operation.
    """
    anyeq_conditions = {}

    def collect_any_eq(node, grouped):
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value == "AnyOpEq":
            # Match only: literal = ANY(identifier)
            if (
                node.left.node_type == NodeType.LITERAL
                and node.right.node_type == NodeType.IDENTIFIER
            ):
                col_id = node.right.schema_column.identity
                if col_id not in grouped:
                    grouped[col_id] = {"values": [], "nodes": [], "column_node": node.right}
                grouped[col_id]["values"].append(node.left.value)
                grouped[col_id]["nodes"].append(node)
            return

        if node.node_type == NodeType.OR:
            collect_any_eq(node.left, grouped)
            collect_any_eq(node.right, grouped)

    collect_any_eq(predicate, anyeq_conditions)

    for data in anyeq_conditions.values():
        if len(data["values"]) > 1:
            statistics.optimization_predicate_rewriter_anyeq_to_contains += 1

            # Build new comparison node: ('a', 'b', 'c') @> z
            new_node = data["nodes"][0]

            new_node.left.value = set(data["values"])
            new_node.left.element_type = new_node.left.type
            new_node.left.type = OrsoTypes.ARRAY
            new_node.left.schema_column = ConstantColumn(
                name=new_node.left.name,
                type=OrsoTypes.ARRAY,
                element_type=new_node.left.element_type,
                value=new_node.left.value,
            )

            new_node.value = "AtArrow"
            new_node.node_type = NodeType.COMPARISON_OPERATOR
            new_node.right = data["column_node"]

            new_node.left, new_node.right = new_node.right, new_node.left  # Swap sides

            # Disable the remaining OR nodes
            for node in data["nodes"][1:]:
                node.node_type = NodeType.LITERAL
                node.type = OrsoTypes.BOOLEAN
                node.value = False

    return predicate


def rewrite_ored_eq_to_inlist(predicate, statistics):
    """
    Rewrite multiple OR'ed Equals conditions on the same column to a single regex pattern.

    Example:
    name = 'Earth' OR name = 'Mars' OR name = 'Venus'
    -->
    name IN ('Earth', 'Mars', 'Venus')
    """
    # Collect Equals conditions that can be combined
    eq_conditions = {}

    def collect_eqs(node, eqs_dict):
        # Base case: LIKE/ILIKE condition
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value in {"Eq"}:
            # Only proceed if the right side is a literal
            if node.right.node_type == NodeType.LITERAL:
                # Get column identifier for grouping
                col_id = None
                if node.left.node_type == NodeType.IDENTIFIER:
                    col_id = node.left.schema_column.identity

                if col_id:
                    if col_id not in eqs_dict:
                        eqs_dict[col_id] = {
                            "values": [],
                            "nodes": [],
                        }

                    eqs_dict[col_id]["values"].append(node.right.value)
                    eqs_dict[col_id]["nodes"].append(node)
            return

        # Recursive cases
        if node.node_type == NodeType.OR:
            collect_eqs(node.left, eqs_dict)
            collect_eqs(node.right, eqs_dict)

    collect_eqs(predicate, eq_conditions)

    for col_id, eq_data in eq_conditions.items():
        if len(eq_data["values"]) > 1:
            statistics.optimization_predicate_rewriter_eqs_to_list += 1
            # Create a new regex pattern
            new_node = eq_data["nodes"][0]
            new_node.value = "InList"
            new_node.right.value = set(eq_data["values"])
            new_node.right.element_type = new_node.right.type
            new_node.right.type = OrsoTypes.ARRAY
            new_node.right.schema_column = ConstantColumn(
                name=new_node.right.name,
                type=OrsoTypes.ARRAY,
                element_type=new_node.right.element_type,
                value=new_node.right.value,
            )
            for node in eq_data["nodes"][1:]:
                node.value = False
                node.node_type = NodeType.LITERAL
                node.type = OrsoTypes.BOOLEAN

    return predicate


# Define dispatcher conditions and actions
dispatcher: Dict[str, Callable] = {
    "rewrite_in_to_eq": rewrite_in_to_eq,
    "reorder_interval_calc": reorder_interval_calc,
}


# Dispatcher conditions
def _rewrite_predicate(predicate, statistics: QueryStatistics):
    if predicate.node_type == NodeType.FUNCTION:
        return _rewrite_function(predicate, statistics)

    # Add our new rewrite for ORed LIKE conditions
    if predicate.node_type == NodeType.OR:
        rewritten = rewrite_ored_like_to_regex(predicate, statistics)
        rewritten = rewrite_ored_eq_to_inlist(rewritten, statistics)
        rewritten = rewrite_ored_any_eq_to_contains(rewritten, statistics)
        if rewritten != predicate:
            return rewritten

    # if predicate.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
    if predicate.left:
        predicate.left = _rewrite_predicate(predicate.left, statistics)
    if predicate.right:
        predicate.right = _rewrite_predicate(predicate.right, statistics)
    if predicate.centre:
        predicate.centre = _rewrite_predicate(predicate.centre, statistics)

    if predicate.node_type not in {NodeType.BINARY_OPERATOR, NodeType.COMPARISON_OPERATOR}:
        # after rewrites, some filters aren't actually predicates
        return predicate

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
                and "%" not in predicate.right.value[1:-1]
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

    if predicate.value == "AnyOpNotEq":
        if predicate.right.node_type == NodeType.LITERAL:
            statistics.optimization_predicate_rewriter_any_to_inlist += 1
            predicate.value = "NotInList"

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
    if function.value == "CASE":
        # CASE WHEN x IS NULL THEN y ELSE x END → IFNULL(x, y)
        if len(function.parameters) == 2 and function.parameters[0].parameters[0].value == "IsNull":
            compare_column = function.parameters[0].parameters[0].centre
            target_column = function.parameters[1].parameters[1]
            value_if_null = function.parameters[1].parameters[0]

            if compare_column.schema_column.identity == target_column.schema_column.identity:
                statistics.optimization_predicate_rewriter_case_to_ifnull += 1
                function.value = "IFNULL"
                function.parameters = [compare_column, value_if_null]
                return function
        # CASE WHEN x THEN y ELSE z END → IIF(x, y, z)
        if (
            len(function.parameters) == 2
            and len(function.parameters[0].parameters) == 2
            and function.parameters[0].parameters[1].value is True
            and len(function.parameters[1].parameters) == 2
        ):
            statistics.optimization_predicate_rewriter_case_to_iif += 1

            compare_column = function.parameters[0].parameters[0]
            value_if_true = function.parameters[1].parameters[0]
            value_if_false = function.parameters[1].parameters[1]

            function.value = "IIF"
            function.parameters = [compare_column, value_if_true, value_if_false]
            return function
    # COALESCE(x, y) → IFNULL(x, y)
    if function.value == "COALESCE":
        if len(function.parameters) == 2:
            statistics.optimization_predicate_rewriter_coalesce_to_ifnull += 1
            function.value = "IFNULL"
            return function
    # SUBSTRING(x, 1, n) → LEFT(x, n)
    if function.value == "SUBSTRING" and function.parameters[1].value == 1:
        statistics.optimization_predicate_rewriter_substring_to_left += 1
        function.value = "LEFT"
        function.parameters = [function.parameters[0], function.parameters[2]]
        return function
    # CONCAT(x, y, z) → x || y || z
    if function.value == "CONCAT" and len(function.parameters) > 1:
        statistics.optimization_predicate_rewriter_concat_to_double_pipe += 1
        left_node = function.parameters[0]
        for param in function.parameters[1:]:
            this_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=left_node,
                right=param,
                schema_column=ExpressionColumn(name="", type=OrsoTypes.VARCHAR),
            )
            left_node = this_node
        this_node.alias = function.alias
        this_node.schema_column = function.schema_column
        function = this_node
    # CONCAT_WS(x, y, z) → y || x || z
    if function.value == "CONCAT_WS" and len(function.parameters) > 2:
        statistics.optimization_predicate_rewriter_concatws_to_double_pipe += 1
        separator = function.parameters[0]
        left_node = function.parameters[1]
        for param in function.parameters[2:]:
            separator_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=left_node,
                right=separator,
                schema_column=ExpressionColumn(name="", type=OrsoTypes.VARCHAR),
            )
            this_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=separator_node,
                right=param,
                schema_column=ExpressionColumn(name="", type=OrsoTypes.VARCHAR),
            )
            left_node = this_node
        this_node.alias = function.alias
        this_node.schema_column = function.schema_column
        function = this_node

    # STARTS_WITH → x LIKE 'pattern%'
    if function.value == "STARTS_WITH":
        statistics.optimization_predicate_rewriter_starts_with_to_like += 1
        left_node = function.parameters[0]
        function.parameters[1].value = function.parameters[1].value + "%"
        this_node = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Like",
            left=left_node,
            right=function.parameters[1],
            schema_column=ExpressionColumn(name="", type=OrsoTypes.BOOLEAN),
        )
        this_node.alias = function.alias
        this_node.schema_column = function.schema_column
        function = this_node

    # ENDS_WITH → x LIKE '%pattern'
    if function.value == "ENDS_WITH":
        statistics.optimization_predicate_rewriter_ends_with_to_like += 1
        left_node = function.parameters[0]
        function.parameters[1].value = "%" + function.parameters[1].value
        this_node = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Like",
            left=left_node,
            right=function.parameters[1],
            schema_column=ExpressionColumn(name="", type=OrsoTypes.BOOLEAN),
        )
        this_node.alias = function.alias
        this_node.schema_column = function.schema_column
        function = this_node

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
                new_columns.append(new_column)
            node.columns = new_columns
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
