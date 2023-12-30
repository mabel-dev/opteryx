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
Optimization Rule - Eliminate Constant Evaluations

Type: Heuristic
Goal: Reduce complexity
"""
import datetime

import numpy
from orso.types import OrsoTypes

from opteryx import operators
from opteryx.functions.binary_operators import binary_operations
from opteryx.managers.expression import NodeType
from opteryx.models.node import Node
from opteryx.third_party.pyarrow_ops.ops import filter_operations


def eliminate_constant_evaluations(plan, properties):
    """
    This action aims to remove repeat evaluations of constant expressions.
    """

    def build_literal_node(value):
        # fmt:off
        if hasattr(value, "as_py"):
            value = value.as_py()
        if value is None:
            return Node(NodeType.LITERAL, type=OrsoTypes.NULL, alias=[])
        if isinstance(value, (bool, numpy.bool_)):
            # boolean must be before numeric
            return Node(NodeType.LITERAL, type=OrsoTypes.BOOLEAN, value=value, alias=[])
        if isinstance(value, (str)):
            return Node(NodeType.LITERAL, type=OrsoTypes.VARCHAR, value=value, alias=[])
        if isinstance(value, int):
            return Node(NodeType.LITERAL, type=OrsoTypes.INTEGER, value=value, alias=[])
        if isinstance(value, (numpy.datetime64, datetime.datetime)):
            return Node(NodeType.LITERAL, type=OrsoTypes.TIMESTAMP, value=value, alias=[])
        if isinstance(value, (datetime.date)):
            return Node(NodeType.LITERAL, type=OrsoTypes.DATE, value=numpy.datetime64(value), alias=[])
        # fmt:on

    def update_expression_tree(node):
        """
        Walk a expression tree collecting all the nodes of a specified type.
        """
        # walk the tree first so we can bubble constants as far up the tree as possible
        node.left = None if node.left is None else update_expression_tree(node.left)
        node.centre = None if node.centre is None else update_expression_tree(node.centre)
        node.right = None if node.right is None else update_expression_tree(node.right)

        if node.parameters:
            node.parameters = [
                parameter if not isinstance(parameter, Node) else update_expression_tree(parameter)
                for parameter in node.parameters
            ]

        # this is the main work of this action
        if (node.left and node.left.node_type == NodeType.LITERAL) and (
            node.right and node.right.node_type == NodeType.LITERAL
        ):
            if node.node_type == NodeType.COMPARISON_OPERATOR:
                value = filter_operations([node.left.value], node.value, [node.right.value])[0]

                return build_literal_node(value)
            if node.node_type == NodeType.BINARY_OPERATOR:
                value = binary_operations([node.left.value], node.value, [node.right.value])[0]
                return build_literal_node(value)
        return node

    # find the in-scope nodes (WHERE AND HAVING)
    selection_nodes = plan.get_nodes_of_type(operators.SelectionNode)

    # killer questions - if any aren't met, bail
    if selection_nodes is None:
        return plan

    # HAVING and WHERE are selection nodes
    for nid in selection_nodes:
        # get the node from the node_id
        operator = plan[nid]
        operator.filter = update_expression_tree(operator.filter)
        if operator.filter is None:
            plan.remove_node(nid, heal=True)

    return plan
