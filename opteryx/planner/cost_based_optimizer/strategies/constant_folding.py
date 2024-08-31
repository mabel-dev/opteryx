# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

import datetime
from typing import Any

import numpy
from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.virtual_datasets import no_table_data

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


def build_literal_node(value: Any, root: Node, suggested_type: OrsoTypes):
    """
    Build a literal node with the appropriate type based on the value.
    """
    # Convert value if it has `as_py` method (e.g., from PyArrow)
    if hasattr(value, "as_py"):
        value = value.as_py()

    if value is None:
        # Matching None has complications
        root.value = None
        root.node_type = NodeType.LITERAL
        root.type = OrsoTypes.NULL
        root.left = None
        root.right = None
        return root

    # Define a mapping of types to OrsoTypes
    type_mapping = {
        bool: OrsoTypes.BOOLEAN,
        numpy.bool_: OrsoTypes.BOOLEAN,
        str: OrsoTypes.VARCHAR,
        numpy.str_: OrsoTypes.VARCHAR,
        bytes: OrsoTypes.BLOB,
        numpy.bytes_: OrsoTypes.BLOB,
        int: OrsoTypes.INTEGER,
        numpy.int64: OrsoTypes.INTEGER,
        float: OrsoTypes.DOUBLE,
        numpy.float64: OrsoTypes.DOUBLE,
        numpy.datetime64: OrsoTypes.TIMESTAMP,
        datetime.datetime: OrsoTypes.TIMESTAMP,
        datetime.time: OrsoTypes.TIME,
        datetime.date: OrsoTypes.DATE,
    }

    value_type = type(value)
    # Determine the type from the value using the mapping
    if value_type in type_mapping or suggested_type not in (OrsoTypes._MISSING_TYPE, 0, None):
        root.value = value
        root.node_type = NodeType.LITERAL
        root.type = (
            suggested_type
            if suggested_type not in (OrsoTypes._MISSING_TYPE, 0, None)
            else type_mapping[value_type]
        )
        root.left = None
        root.right = None

    # DEBUG:log (f"Unable to create literal node for {value}, of type {value_type}")
    return root


def fold_constants(root: Node) -> Node:
    if root.node_type == NodeType.LITERAL:
        # if we're already a literal (constant), we can't fold
        return root

    if root.node_type in {NodeType.COMPARISON_OPERATOR, NodeType.BINARY_OPERATOR}:
        # if we have a binary expression, try to fold each side
        root.left = fold_constants(root.left)
        root.right = fold_constants(root.right)

        # some expressions we can simplify to x or 0.
        if root.node_type == NodeType.BINARY_OPERATOR:
            if (
                root.value == "Multiply"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 0
            ):
                # 0 * anything = 0
                root.left.schema_column = root.schema_column
                return root.left  # 0
            if (
                root.value == "Multiply"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 0
            ):
                # anything * 0 = 0
                root.right.schema_column = root.schema_column
                return root.right  # 0
            if (
                root.value == "Multiply"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 1
            ):
                # 1 * anything = anything
                root.right.query_column = root.query_column
                return root.right  # anything
            if (
                root.value == "Multiply"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 1
            ):
                # anything * 1 = anything
                root.left.query_column = root.query_column
                return root.left  # anything
            if (
                root.value in "Plus"
                and root.left.node_type == NodeType.LITERAL
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.value == 0
            ):
                # 0 + anything = anything
                root.right.query_column = root.query_column
                return root.right  # anything
            if (
                root.value in ("Plus", "Minus")
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 0
            ):
                # anything +/- 0 = anything
                root.left.query_column = root.query_column
                return root.left  # anything
            if (
                root.value == "Divide"
                and root.right.node_type == NodeType.LITERAL
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.value == 1
            ):
                # anything / 1 = anything
                root.left.schema_column = root.schema_column
                return root.left  # anything
            if (
                root.value == "Divide"
                and root.right.node_type == NodeType.IDENTIFIER
                and root.left.node_type == NodeType.IDENTIFIER
                and root.right.schema_column.identity == root.left.schema_column.identity
            ):
                # anything / itself = 1 (0 is an exception)
                node = build_literal_node(1, root, OrsoTypes.INTEGER)
                node.schema_column = root.schema_column
                return node

    if root.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
        # try to fold each side of logical operators
        root.left = fold_constants(root.left)
        root.right = fold_constants(root.right)

        # If we have a logical expression and one side is a constant,
        # we can simplify further
        if root.node_type == NodeType.OR:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and root.left.value
            ):
                # True OR anything is True
                root.left.schema_column = root.schema_column
                return root.left
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and root.right.value
            ):
                # anything OR True is True
                root.right.schema_column = root.schema_column
                return root.right
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and not root.left.value
            ):
                # False OR anything is anything
                root.right.schema_column = root.schema_column
                return root.right
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and not root.right.value
            ):
                # anything OR False is anything
                root.left.schema_column = root.schema_column
                return root.left

        elif root.node_type == NodeType.AND:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and not root.left.value
            ):
                # False AND anything is False
                root.left.schema_column = root.schema_column
                return root.left
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and not root.right.value
            ):
                # anything AND False is False
                root.right.schema_column = root.schema_column
                return root.right
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and root.left.value
            ):
                # True AND anything is anything
                root.right.schema_column = root.schema_column
                return root.right
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and root.right.value
            ):
                # anything AND True is anything
                root.left.schema_column = root.schema_column
                return root.left

        return root

    identifiers = get_all_nodes_of_type(root, (NodeType.IDENTIFIER, NodeType.WILDCARD))
    functions = get_all_nodes_of_type(root, (NodeType.FUNCTION,))
    aggregators = get_all_nodes_of_type(root, (NodeType.AGGREGATOR,))

    if any(func.value in {"RANDOM", "RAND", "NORMAL", "RANDOM_STRING"} for func in functions):
        # Although they have no params, these are evaluated per row
        return root

    if len(identifiers) == 0 and len(aggregators) == 0:
        table = no_table_data.read()
        try:
            result = evaluate(root, table)[0]
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
            node.condition = fold_constants(node.condition)
            if node.condition.node_type == NodeType.LITERAL and node.condition.value:
                context.optimized_plan.remove_node(context.node_id, heal=True)
            else:
                context.optimized_plan[context.node_id] = node
        # fold constants when referenced in the SELECT clause
        if node.node_type == LogicalPlanStepType.Project:
            node.columns = [fold_constants(c) for c in node.columns]
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
