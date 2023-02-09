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
Optimization Rule - Predicate Pushdown

Type: Heuristic
Goal: Reduce rows
"""
from opteryx import config
from opteryx import operators
from opteryx.managers.expression import NodeType


def predicate_pushdown(plan, properties):
    """
    Initial implementation of Predicate Pushdown, this puts selections (filters) as
    close to the read as possible, including offloading them to external services.

    This has the benefit that we have less data to pass around the system, and
    where we can offload to other systems (e.g. FireStore) we don't need to
    transport that data over the network.

    We do this work by removing the selection node from the query plan and adding
    it to the filter in the reader node.

    This initial implementation has a lot of limitations:

    - only a single reader can exist in the plan
    - only some readers support pushdown
    - no GROUP BY / Aggregate can exist in the plan

    """
    # find the in-scope nodes
    selection_nodes = plan.get_nodes_of_type(operators.SelectionNode)
    reader_nodes = plan.get_nodes_of_type(
        (
            operators.BlobReaderNode,
            operators.CollectionReaderNode,
            operators.SqlReaderNode,
        )
    )

    # killer questions - if any aren't met, bail
    if selection_nodes is None:
        return plan
    if len(plan.get_nodes_of_type(operators.AggregateNode)) > 0:
        # don't try to work out if it's a HAVING or GROUP BY
        # a HAVING without a GROUP BY should be okay
        return plan
    if len(reader_nodes) != 1:
        # don't try to work out which reader to push to
        return plan
    reader_node = plan[reader_nodes[0]]
    if not reader_node.can_push_selection:
        # not all readers support pushdown
        return plan

    # WHERE are selection nodes
    for nid in selection_nodes:
        # get the node from the node_id
        operator = plan[nid]
        # only add simple predicates (makes ANDs)
        if operator.filter.token_type == NodeType.COMPARISON_OPERATOR:
            if config.ONLY_PUSH_EQUALS_PREDICATES and operator.filter.value != "Eq":
                continue
            if reader_node.push_predicate(operator.filter):
                plan.remove_node(nid, heal=True)

    return plan
