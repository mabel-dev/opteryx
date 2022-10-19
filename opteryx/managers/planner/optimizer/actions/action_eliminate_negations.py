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
Optimization Rule - Eliminate Negations

Type: Heuristic
Goal: Reduce complexity
"""
from opteryx import operators
from opteryx.managers.expression import ExpressionTreeNode, NodeType


def eliminate_negations(plan, properties):
    """
    This action aims to remove steps from the execution of an expression by eliminating
    negations. This is triggered by a NOT in an expression, when this is observed we
    look at what it is negating to see if we can rewrite it so the NOT isn't required.

    This takes the form of removing chained NOTs - this is not actually expected in
    actual use - and seeing if we can invert a condition -
        e.g. 'NOT a = b' == 'a != b'
    """

    # Operations safe to invert.
    inversions: dict = {
        "Eq": "NotEq",
        "NotEq": "Eq",
        "Gt": "LtEq",
        "GtEq": "Lt",
        "Lt": "GtEq",
        "LtEq": "Gt",
    }

    def update_expression_tree(node):
        """
        Walk a expression tree collecting all the nodes of a specified type.
        """
        # this is the main work of this action
        if node.token_type == NodeType.NESTED:
            return update_expression_tree(node.centre)
        if node.token_type == NodeType.NOT:
            centre_node = node.centre
            # a straight forward eliminate NOT(NOT(x))
            if centre_node.token_type == NodeType.NOT:
                return update_expression_tree(centre_node.centre)
            # do we have an invertable operator
            if (
                centre_node.token_type == NodeType.COMPARISON_OPERATOR
                and centre_node.value in inversions
            ):
                centre_node.value = inversions[centre_node.value]
                return update_expression_tree(centre_node)
        # below here is generic to walk the tree
        node.left = None if node.left is None else update_expression_tree(node.left)
        node.centre = (
            None if node.centre is None else update_expression_tree(node.centre)
        )
        node.right = None if node.right is None else update_expression_tree(node.right)
        if node.parameters:
            node.parameters = [
                parameter
                if not isinstance(parameter, ExpressionTreeNode)
                else update_expression_tree(parameter)
                for parameter in node.parameters
            ]
        return node

    # find the in-scope nodes (WHERE AND HAVING)
    selection_nodes = plan.get_nodes_of_type(operators.SelectionNode)

    # killer questions - if any aren't met, bail
    if selection_nodes is None:
        return plan

    # HAVING and WHERE are selection nodes
    for nid in selection_nodes:
        # get the node from the node_id
        operator = plan.get_operator(nid)
        operator.filter = update_expression_tree(operator.filter)

    return plan
