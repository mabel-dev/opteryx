# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Constant Folding

Type: Heuristic
Goal: Evaluate Once

We identify branches in expressions where there are no identifiers, these usually
mean we can evaluate them once, in the optimization phase, and replace them with a
constant for handling in the execution phase, reducing the amount of work done by
the execution engine.

We run this strategy twice, once at the beginning, which primarily handles user
entered expressions we can optimize, and again at the end which handles where
we've rewritten expressions at part of other optimizations which can be folded.
"""

from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.models import QueryStatistics
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.virtual_datasets import no_table_data

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


def _build_if_not_null_node(root, value, value_if_not_null) -> Node:
    node = Node(node_type=NodeType.FUNCTION)
    node.value = "IFNOTNULL"
    node.parameters = [value, value_if_not_null]
    node.schema_column = root.schema_column
    node.query_column = root.query_column
    return node


def _build_passthru_node(root, value) -> Node:
    if root.node_type == NodeType.COMPARISON_OPERATOR:
        return root

    node = Node(node_type=NodeType.FUNCTION)
    node.value = "PASSTHRU"
    node.parameters = [value]
    node.schema_column = root.schema_column
    node.query_column = root.query_column
    return node


def fold_constants(root: Node, statistics: QueryStatistics) -> Node:
    if root.node_type == NodeType.LITERAL:
        # if we're already a literal (constant), we can't fold
        return root

    if root.node_type == NodeType.EXPRESSION_LIST:
        # we currently don't fold CASE expressions
        return root

    if root.node_type in {NodeType.COMPARISON_OPERATOR, NodeType.BINARY_OPERATOR}:
        # if we have a binary expression, try to fold each side
        root.left = fold_constants(root.left, statistics)
        root.right = fold_constants(root.right, statistics)

        # some expressions we can simplify to x or 0.
        if root.node_type == NodeType.BINARY_OPERATOR:
            if (
                root.value == "Multiply"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 0
            ):
                # 0 * anything = 0 (except NULL)
                node = _build_if_not_null_node(root, root.right, build_literal_node(0))
                statistics.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Multiply"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 0
            ):
                # anything * 0 = 0 (except NULL)
                node = _build_if_not_null_node(root, root.left, build_literal_node(0))
                statistics.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Multiply"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 1
            ):
                # 1 * anything = anything (except NULL)
                node = _build_passthru_node(root, root.right)
                statistics.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Multiply"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 1
            ):
                # anything * 1 = anything (except NULL)
                node = _build_passthru_node(root, root.left)
                statistics.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value in "Plus"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 0
            ):
                # 0 + anything = anything (except NULL)
                node = _build_passthru_node(root, root.right)
                statistics.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value in ("Plus", "Minus")
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 0
            ):
                # anything +/- 0 = anything (except NULL)
                node = _build_passthru_node(root, root.left)
                statistics.optimization_constant_fold_reduce += 1
                return node
            if (
                root.value == "Divide"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 1
            ):
                # anything / 1 = anything (except NULL)
                node = _build_passthru_node(root, root.left)
                statistics.optimization_constant_fold_reduce += 1
                return node

        if root.node_type == NodeType.COMPARISON_OPERATOR:
            # anything LIKE '%' is true for non null values
            if (
                root.value in ("Like", "ILike")
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.node_type == NodeType.LITERAL
                and root.right.value == "%"
            ):
                # column LIKE '%' is True
                node = Node(node_type=NodeType.UNARY_OPERATOR)
                node.type = OrsoTypes.BOOLEAN
                node.value = "IsNotNull"
                node.schema_column = root.schema_column
                node.centre = root.left
                node.query_column = root.query_column
                node.alias = root.alias
                statistics.optimization_constant_fold_reduce += 1
                return node

    if root.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
        # try to fold each side of logical operators
        root.left = fold_constants(root.left, statistics)
        root.right = fold_constants(root.right, statistics)

        # If we have a logical expression and one side is a constant,
        # we can simplify further
        if root.node_type == NodeType.OR:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and root.left.value
            ):
                # True OR anything is True (including NULL)
                node = _build_passthru_node(root, root.left)
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and root.right.value
            ):
                # anything OR True is True (including NULL)
                node = _build_passthru_node(root, root.right)
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and not root.left.value
            ):
                # False OR anything is anything (except NULL)
                node = _build_passthru_node(root, root.right)
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and not root.right.value
            ):
                # anything OR False is anything (except NULL)
                node = _build_passthru_node(root, root.left)
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node

        elif root.node_type == NodeType.AND:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and not root.left.value
            ):
                # False AND anything is False (including NULL)
                node = _build_passthru_node(root, root.left)
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and not root.right.value
            ):
                # anything AND False is False (including NULL)
                node = _build_passthru_node(root, root.right)
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and root.left.value
            ):
                # True AND anything is anything (except NULL)
                node = _build_passthru_node(root, root.right)
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and root.right.value
            ):
                # anything AND True is anything (except NULL)
                node = _build_passthru_node(root, root.left)
                node.type = OrsoTypes.BOOLEAN
                statistics.optimization_constant_fold_boolean_reduce += 1
                return node

        return root

    identifiers = get_all_nodes_of_type(root, (NodeType.IDENTIFIER, NodeType.WILDCARD))
    functions = get_all_nodes_of_type(root, (NodeType.FUNCTION,))
    aggregators = get_all_nodes_of_type(root, (NodeType.AGGREGATOR,))

    if any(func.value in ("RANDOM", "RAND", "NORMAL", "RANDOM_STRING") for func in functions):
        # Although they have no params, these are evaluated per row
        return root

    # fold costants in function parameters - this is generally aggregations we're affecting here
    if root.parameters:
        if isinstance(root.parameters, tuple):
            root.parameters = list(root.parameters)
        for i, param in enumerate(root.parameters):
            root.parameters[i] = fold_constants(param, statistics)

    # rewrite aggregations to constants where possible
    for agg in aggregators:
        if len(agg.parameters) == 1 and agg.parameters[0].node_type == NodeType.LITERAL:
            if agg.value == "COUNT":
                # COUNT(1) is always the number of rows
                root.parameters[0] = Node(NodeType.WILDCARD)
                statistics.optimization_constant_aggregation += 1
                return root
            if agg.value in ("AVG", "MIN", "MAX"):
                # AVG, MIN, MAX of a constant is the constant
                statistics.optimization_constant_aggregation += 1
                return build_literal_node(agg.parameters[0].value, root, root.schema_column.type)

    if len(identifiers) == 0 and len(aggregators) == 0:
        table = no_table_data.read()
        try:
            result = evaluate(root, table)[0]
            statistics.optimization_constant_fold_expression += 1
            return build_literal_node(result, root, root.schema_column.type)
        except (ValueError, TypeError, KeyError) as err:  # nosec
            if not err:
                pass
            # what ever the reason, just skip
            # DEBUG:log (err)
    return root


class ConstantFoldingStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Constant Folding is when we precalculate expressions (or sub expressions)
        which contain only constant or literal values.
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        # fold constants when referenced in filter clauses (WHERE/HAVING)
        if node.node_type == LogicalPlanStepType.Filter:
            node.condition = fold_constants(node.condition, self.statistics)
            if node.condition.node_type == NodeType.LITERAL and node.condition.value:
                context.optimized_plan.remove_node(context.node_id, heal=True)
            else:
                context.optimized_plan[context.node_id] = node
        # fold constants when referenced in the SELECT clause
        if node.node_type == LogicalPlanStepType.Project:
            node.columns = [fold_constants(c, self.statistics) for c in node.columns]
            context.optimized_plan[context.node_id] = node

        # remove nesting in order by and group by clauses
        if node.node_type == LogicalPlanStepType.Order:
            new_order_by = []
            for field, order in node.order_by:
                while field.node_type == NodeType.NESTED:
                    field = field.centre
                new_order_by.append((field, order))
            node.order_by = new_order_by
            context.optimized_plan[context.node_id] = node

        if node.node_type == LogicalPlanStepType.AggregateAndGroup:
            node.groups = [g.centre if g.node_type == NodeType.NESTED else g for g in node.groups]
            node.groups = [fold_constants(g, self.statistics) for g in node.groups]
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
