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
Optimization Rule - Split Commutive Predicates (ANDs)
"""

from opteryx import operators


def run(plan):
    """
    Commutive Predicates are those that the order of their evaluation doesn't matter.

    The reason for splitting is two-fold:

    1)  Smaller expressions are easier to move around the query plan as they have fewer
        dependencies.
    2)  Executing predicates like this means susequent predicates will be operating on
        fewer records, which is generally faster
    """
    # find the in-scope nodes
    selection_nodes = plan.get_nodes_of_type(operators.SelectionNode)

    # killer questions - if any aren't met, bale
    if selection_nodes is None:
        return plan

    # HAVING and WHERE are selection nodes
    for selection_node in selection_nodes:
        pass
    # get the expression out of the selection nodes
    # if the root is an AND split into two and repeat (i.e. if they have an AND root)
    # remove the node from the naive plan and insert the new nodes

    return plan
