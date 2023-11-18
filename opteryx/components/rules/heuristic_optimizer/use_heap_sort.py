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
Optimization Rule - Combine Operators for Effeciency

Type: Heuristic
Goal: Reduce rows
"""
from opteryx import operators
from opteryx.models.execution_tree import ExecutionTree
from opteryx.utils import random_string


def use_heap_sort(plan: ExecutionTree, properties):
    """
    This action merges sort and limit nodes to use an algorithm which performs both
    faster than if they are performed as separate steps.
    """

    # find the in-scope nodes
    sort_nodes = plan.get_nodes_of_type(operators.SortNode)

    # killer questions - if any aren't met, bail
    if sort_nodes is None:
        return plan

    for nid in sort_nodes:
        next_nids = plan.outgoing_edges(nid)
        if len(next_nids) != 1:
            continue
        next_nid = next_nids.pop()[1]
        limit_node = plan[next_nid]
        if isinstance(limit_node, operators.LimitNode):
            sort_node = plan[nid]
            heap_sort = operators.HeapSortNode(
                properties=properties, order=sort_node.order, limit=limit_node.limit
            )
            plan.insert_node_before(f"heap-sort-{random_string()}", heap_sort, nid)
            plan.remove_node(nid, heal=True)
            plan.remove_node(next_nid, heal=True)

    return plan
