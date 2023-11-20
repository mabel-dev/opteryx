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
Optimization Rule - Order Predicates

Type: Heuristic
Goal: Reduce rows
"""

from opteryx.managers.expression import NodeType

NODE_ORDER = {
    "Eq": 1,
    "NotEq": 1,
    "Gt": 2,
    "GtEq": 2,
    "Lt": 2,
    "LtEq": 2,
    "Like": 4,
    "ILike": 4,
    "NotLike": 4,
    "NotILike": 4,
}


def rule_order_predicates(nodes):
    """
    Ordering of predicates based on rules, this is mostly useful for situations where
    we do not have statistics to make cost-based decisions.

    We're going to start with arbitrary numbers, we need to find a way to refine these
    over time.
    """

    for node in nodes:
        if not node.node_type == NodeType.COMPARISON_OPERATOR:
            node.score = 25
            continue
        node.score = NODE_ORDER.get(node.value, 12)
        if node.left.node_type == NodeType.LITERAL:
            node.score += 1
        elif node.left.node_type == NodeType.IDENTIFIER:
            node.score += 3
        else:
            node.score += 10
        if node.right.node_type == NodeType.LITERAL:
            node.score += 1
        elif node.right.node_type == NodeType.IDENTIFIER:
            node.score += 3
        else:
            node.score += 10

    return sorted(nodes, key=lambda node: node.score)
