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
Optimization Rule - Demorgan's Laws

Type: Heuristic
Goal: Preposition for following actions
"""
from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# Operations safe to invert.
HALF_INVERSIONS: dict = {
    "Eq": "NotEq",
    "Gt": "LtEq",
    "GtEq": "Lt",
    "Like": "NotLike",
    "ILike": "NotILike",
    "SimilarTo": "NotSimilarTo",
    "PGRegexMatch": "NotPGRegexMatch",
    "PGRegexIMatch": "PGRegexIMatch",
    # "AnyOpEq": "AnyOpNotEq",
    # "IsFalse": is_compare,
    # "IsNotFalse": is_compare,
    # "IsNotNull": is_compare,
    # "IsNotTrue": is_compare,
    # "IsNull": is_compare,
    # "IsTrue": is_compare,
}

INVERSIONS = {**HALF_INVERSIONS, **{v: k for k, v in HALF_INVERSIONS.items()}}


class BooleanSimplificationStrategy(OptimizationStrategy):  # pragma: no cover
    """
    This action aims to rewrite and simplify expressions.

    This has two purposes:
     1) Reduce the work to evaluate expressions by removing steps
     2) Express conditions in ways that other strategies can act on, e.g. pushing
        predicates.

    The core of this action taking advantage of the following:

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
            node.condition = update_expression_tree(node.condition)
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan


def update_expression_tree(node):

    # break out of nests
    if node.node_type == NodeType.NESTED:
        return update_expression_tree(node.centre)

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
            return update_expression_tree(Node(NodeType.AND, left=a_side, right=b_side))

        # NOT(A = B) => A != B
        if centre_node.value in INVERSIONS:
            centre_node.value = INVERSIONS[centre_node.value]
            return update_expression_tree(centre_node)

        # NOT(NOT(A)) => A
        if centre_node.node_type == NodeType.NOT:
            return update_expression_tree(centre_node.centre)

    # traverse the expression tree
    node.left = None if node.left is None else update_expression_tree(node.left)
    node.centre = None if node.centre is None else update_expression_tree(node.centre)
    node.right = None if node.right is None else update_expression_tree(node.right)
    if node.parameters:
        node.parameters = [
            parameter if not isinstance(parameter, Node) else update_expression_tree(parameter)
            for parameter in node.parameters
        ]

    return node
