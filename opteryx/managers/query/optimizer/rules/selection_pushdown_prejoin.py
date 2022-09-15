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
Optimization Rule - Selection Pushdown PreJoin
"""


from opteryx import operators
from opteryx.managers.query.optimizer import get_matching_plan_operators


def run(plan):
    """
    The naive order of a query execution puts the WHERE execution after the JOIN
    execution.
    
    This means that most of the time when these two exist in the same query, the JOIN
    is doing work that is thrown-away almost immediately. This optimization step
    attempts to reduce this by trying to identify when it can move the filter to before
    the join.
    """


    # find the in-scope nodes
    selection_nodes = get_matching_plan_operators(plan, operators.SelectionNode)
    join_nodes = get_matching_plan_operators(
        plan, (operators.InnerJoinNode, operators.OuterJoinNode)
    )

    # killer questions
    if selection_nodes is None:
        return plan
    if join_nodes is None:
        return plan

    # just because we're here - doesn't mean we can optimize

    #    walk the DAG

    return plan
