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
Optimization Rule - Eliminite Fixed-Outcome Function Evaluations

Type: Heuristic
Goal: Reduce complexity
"""
from opteryx import operators
from opteryx.functions import date_functions, get_version
from opteryx.managers.expression import ExpressionTreeNode, NodeType

FIXED_OUTCOME_FUNCTIONS = {
    "CURRENT_TIME": (date_functions.get_now, NodeType.LITERAL_TIMESTAMP),
    "NOW": (date_functions.get_now, NodeType.LITERAL_TIMESTAMP),
    "CURRENT_DATE": (date_functions.get_today, NodeType.LITERAL_TIMESTAMP),
    "TODAY": (date_functions.get_today, NodeType.LITERAL_TIMESTAMP),
    "TIME": (date_functions.get_time, NodeType.LITERAL_TIMESTAMP),
    "YESTERDAY": (date_functions.get_yesterday, NodeType.LITERAL_TIMESTAMP),
    "VERSION": (get_version, NodeType.LITERAL_VARCHAR),
}


def eliminate_fixed_function_evaluations(plan, properties):
    """
    This action aims to execute functions once and replace the outcome as a literal in
    the query, where the function evaluation will be the same for the query, for
    example `current_time`
    """

    def update_expression_tree(node):
        """
        Walk a expression tree collecting all the nodes of a specified type.
        """

        # this is the main work of this action
        if (
            node.token_type == NodeType.FUNCTION
            and node.value in FIXED_OUTCOME_FUNCTIONS
        ):
            function, node_type = FIXED_OUTCOME_FUNCTIONS[node.value]
            return ExpressionTreeNode(node_type, value=function(), alias=node.alias)

        # walk the tree
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
