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


def build_literal_node(value: Any, root: Node):
    # fmt:off
    if hasattr(value, "as_py"):
        value = value.as_py()

    root.value = value
    root.node_type = NodeType.LITERAL
    if value is None:
        root.type=OrsoTypes.NULL
    elif isinstance(value, (bool, numpy.bool_)):
        # boolean must be before numeric
        root.type=OrsoTypes.BOOLEAN
    elif isinstance(value, (str)):
        root.type=OrsoTypes.VARCHAR
    elif isinstance(value, (int, numpy.int64)):
        root.type=OrsoTypes.INTEGER
    elif isinstance(value, (numpy.datetime64, datetime.datetime)):
        root.type=OrsoTypes.TIMESTAMP
    elif isinstance(value, (datetime.date)):
        root.type=OrsoTypes.DATE
    else:
        raise ValueError("Unable to fold expression")
    return root
    # fmt:on


def fold_constants(root: Node) -> Node:
    if root.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
        # traverse the left and right of logical operators
        root.left = fold_constants(root.left)
        root.right = fold_constants(root.right)

        # is we have a logical expression and one side is a constant, 
        # we simplify the expression
        if root.node_type == NodeType.OR:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and root.left.value
            ):
                # True OR anything is True
                return root.left
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and root.right.value
            ):
                # True OR anything is True
                return root.right
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and not root.left.value
            ):
                # False OR x is x
                return root.right
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and not root.right.value
            ):
                return root.left

        elif root.node_type == NodeType.AND:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and not root.left.value
            ):
                # False AND anything is False
                return root.left
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and not root.right.value
            ):
                return root.right
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and root.left.value
            ):
                # True AND x is x
                return root.right
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and root.right.value
            ):
                return root.left

        elif root.node_type == NodeType.XOR:
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and root.left.value
            ):
                # True XOR x is NOT x
                return Node(NodeType.NOT, root.right)
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and root.right.value
            ):
                return Node(NodeType.NOT, root.left)
            if (
                root.left.node_type == NodeType.LITERAL
                and root.left.type == OrsoTypes.BOOLEAN
                and not root.left.value
            ):
                # False XOR x is x
                return root.right
            if (
                root.right.node_type == NodeType.LITERAL
                and root.right.type == OrsoTypes.BOOLEAN
                and not root.right.value
            ):
                return root.left

        return root

    identifiers = get_all_nodes_of_type(root, (NodeType.IDENTIFIER, NodeType.WILDCARD))
    functions = get_all_nodes_of_type(root, (NodeType.FUNCTION,))

    if any(func.value in {"RANDOM", "RAND", "NORMAL", "RANDOM_STRING"} for func in functions):
        # although they have no params, these need to evaluate every time
        return root

    if len(identifiers) == 0:
        table = no_table_data.read()
        try:
            result = evaluate(root, table, None)[0]
            return build_literal_node(result, root)
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

        if node.node_type == LogicalPlanStepType.Filter:
            node.condition = fold_constants(node.condition)
            if node.condition.node_type == NodeType.LITERAL and node.condition.value:
                context.optimized_plan.remove_node(context.node_id, heal=True)
            else:
                context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
