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
Optimization Rule - Split Conjunctive Predicates (ANDs)

Type: Heuristic
Goal: Reduce rows
"""
from opteryx import operators
from opteryx.managers.expression import NodeType


def split_conjunctive_predicates(plan, properties):
    """
    Conjunctive Predicates (ANDs) can be split and executed in any order to get the
    same result. This means we can split them into separate steps in the plan.

    The reason for splitting is two-fold:

    1)  Smaller expressions are easier to move around the query plan as they have fewer
        dependencies.
    2)  Executing predicates like this means each runs in turn, filtering out some of
        the records meaning susequent predicates will be operating on fewer records,
        which is generally faster. We can also order these predicates to get a faster
        result, balancing the selectivity (get rid of more records faster) vs cost of
        the check (a numeric check is faster than a string check)
    """

    def unique_id():
        import random

        return hex(random.getrandbits(16))

    def _inner_split(plan, nid, operator):

        selection = operator.filter
        if selection.token_type != NodeType.AND:
            return plan

        # get the left and right filters
        left_node = operators.SelectionNode(
            filter=selection.left, properties=properties
        )
        right_node = operators.SelectionNode(
            filter=selection.right, properties=properties
        )
        # insert them into the plan and remove the old node
        # we're chaining the new operators
        uid = unique_id()  # avoid collisions
        plan.insert_operator_before(f"{nid}-{uid}-right", right_node, nid)
        plan.insert_operator_before(
            f"{nid}-{uid}-left", left_node, f"{nid}-{uid}-right"
        )
        plan.remove_operator(nid)

        # recurse until we get to a non-AND condition
        plan = _inner_split(plan, f"{nid}-{uid}-right", right_node)
        plan = _inner_split(plan, f"{nid}-{uid}-left", left_node)

        return plan

    # find the in-scope nodes
    selection_nodes = plan.get_nodes_of_type(operators.SelectionNode)

    # killer questions - if any aren't met, bail
    if selection_nodes is None:
        return plan

    # HAVING and WHERE are selection nodes
    for nid in selection_nodes:
        # get the node from the node_id
        operator = plan.get_operator(nid)
        plan = _inner_split(plan, nid, operator)

    return plan
