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


import numpy
from orso.types import OrsoTypes

from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.virtual_datasets import no_table_data

from .optimization_strategy import HeuristicOptimizerContext
from .optimization_strategy import OptimizationStrategy


class InSubQueryToJoinStrategy(OptimizationStrategy):
    def visit(
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> HeuristicOptimizerContext:
        """
        Constant Folding is when we precalculate expressions (or sub expressions)
        which contain only constant or literal or literal values. These don't
        tend to happen IRL, but it's a simple enough strategy so should be
        included.
        """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        #        if node.node_type == LogicalPlanStepType.Filter:
        #            print(node.condition)

        return context

    def complete(self, plan: LogicalPlan, context: HeuristicOptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
