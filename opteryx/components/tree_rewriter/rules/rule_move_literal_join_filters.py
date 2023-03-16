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
Rewrite Rule - Literal Join Filters

Goal: Move filters JOIN which reference literals
"""
from opteryx import operators
from opteryx.managers.expression import LITERAL_TYPE
from opteryx.managers.expression import NodeType
from opteryx.utils import random_string


def move_literal_join_filters(plan, properties):
    """
    This action aims to move any filters in JOIN ON conditions which are referencing
    literals and therefore aren't actually a join condition.

    If the condition doesn't have a literal, then we assume it's a join condition.

    When we remove the filters, we put them after the join - this is always correct
    but non-optimal, that's fine we should let the optimizer work out where to filter.
    """

    def _has_literal(node):
        return (node.left and node.left.token_type & LITERAL_TYPE == LITERAL_TYPE) or (
            node.right and node.right.token_type & LITERAL_TYPE == LITERAL_TYPE
        )

    def _move_literal_filters(plan, nid, node):
        # we need to be at an AND
        if node.token_type != NodeType.AND:
            return node, plan

        uid = random_string()  # avoid collisions
        if _has_literal(node.left):
            # we create a new selection node with the literal filter and add it to the plan
            new_node = operators.SelectionNode(filter=node.left, properties=properties)
            plan.insert_node_after(f"select-{nid}-{uid}-left", new_node, nid)
            # we remove the filter from this node
            node = node.right
        elif _has_literal(node.right):
            new_node = operators.SelectionNode(filter=node.right, properties=properties)
            plan.insert_node_after(f"select-{nid}-{uid}-right", new_node, nid)
            # we remove the filter from this node
            node = node.left

        return node, plan

    # find the in-scope nodes (INNER and OUTER joins)
    join_nodes = plan.get_nodes_of_type([operators.InnerJoinNode, operators.OuterJoinNode])

    # HAVING and WHERE are selection nodes
    for nid in join_nodes:
        # get the node from the node_id
        operator = plan[nid]
        if operator._on is not None:
            operator._on, plan = _move_literal_filters(plan, nid, operator._on)

    return plan
