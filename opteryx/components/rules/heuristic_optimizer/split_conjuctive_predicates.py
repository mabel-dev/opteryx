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

from opteryx.managers.expression import NodeType


def rule_split_conjunctive_predicates(node):
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

    def _inner_split(node):
        if node.node_type != NodeType.AND:
            return [node]

        # get the left and right filters
        left_nodes = _inner_split(node.left)
        right_nodes = _inner_split(node.right)

        return left_nodes + right_nodes

    return _inner_split(node.condition)
