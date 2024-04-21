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

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode


# Context object to carry state
class OptimizerContext:
    def __init__(self, tree: LogicalPlan):
        self.node_id = None
        self.parent_nid = None
        self.last_nid = None
        self.pre_optimized_tree = tree
        self.optimized_plan = LogicalPlan()

        # We collect predicates we should be able to push to reads and joins
        self.collected_predicates: list = []

        # We collect column identities so we can push column selection as close to the
        # read as possible, including off to remote systems
        self.collected_identities: set = set()


class OptimizationStrategy:
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        raise NotImplementedError(
            "Visit method must be implemented in OptimizationStrategy classes."
        )

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        raise NotImplementedError(
            "Complete method must be implemented in OptimizationStrategy classes."
        )
