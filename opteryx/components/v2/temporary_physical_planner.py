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
This is a temporary step, which takes logical plans from the V2 planner
and converts them to V1 physical plans.
"""

from opteryx import operators
from opteryx.models import ExecutionTree


def create_physical_plan(logical_plan):
    plan = ExecutionTree()

    for nid, logical_node in logical_plan.nodes(data=True):
        plan.add_node(
            nid,
            operators.SelectionNode("properties", filter="_selection"),
        )

    for source, destination, relation in logical_plan.edges():
        plan.add_edge(source, destination)

    return plan
