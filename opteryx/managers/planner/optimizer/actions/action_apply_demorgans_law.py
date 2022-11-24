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
from opteryx import operators
from opteryx.managers.expression import ExpressionTreeNode, NodeType


def apply_demorgans_law(plan, properties):
    """
    This action aims to create more opportunity for other rules to act on. Demorgan's
    Laws allow the conversion of ORs to ANDs through Negations.

    By converting ORs to ANDs the 'split conjuctive predicates' action will have more
    to act on. By changing how the Negations are expressed, the 'eliminate negations'
    action will have more to act on.

    The core of this action is taking advantage of the following:

        not (A or B) = (not A) and (not B)
    """

    def update_expression_tree(node):
        """
        Walk a expression tree collecting all the nodes of a specified type.
        """
        # this is the main work of this action
        if node.token_type == NodeType.NESTED:
            return update_expression_tree(node.centre)
        if node.token_type == NodeType.NOT:
            centre_node = node.centre

            # break out of nesting
            if centre_node.token_type == NodeType.NESTED:
                centre_node = centre_node.centre

            # do we have a NOT (a or b)?
            if centre_node.token_type == NodeType.OR:
                # rewrite to (not A) and (not B)
                a_side = ExpressionTreeNode(
                    NodeType.NOT, centre=update_expression_tree(centre_node.left)
                )
                b_side = ExpressionTreeNode(
                    NodeType.NOT, centre=update_expression_tree(centre_node.right)
                )
                return ExpressionTreeNode(NodeType.AND, left=a_side, right=b_side)

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
