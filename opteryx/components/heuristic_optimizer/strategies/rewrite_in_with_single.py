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

from orso.tools import random_string
from orso.types import PYTHON_TO_ORSO_MAP

from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import ORSO_TO_NUMPY_MAP
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type

from .optimization_strategy import HeuristicOptimizerContext
from .optimization_strategy import OptimizationStrategy

REWRITES = {"InList": "Eq", "NotInList": "NotEq"}


def _rewriter(node):

    if node.value in REWRITES:
        if node.right.node_type == NodeType.LITERAL and len(node.right.value) == 1:
            node.value = REWRITES[node.value]
            node.right.value = node.right.value.pop()
            node.right.type = node.right.sub_type
            node.right.sub_type = None

    elif node.node_type in (NodeType.AND, NodeType.OR):
        node.left = _rewriter(node.left)
        node.right = _rewriter(node.right)

    return node


class RewriteInWithSingleComparitorStrategy(OptimizationStrategy):
    def visit(
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> HeuristicOptimizerContext:
        """
        Rewrite IN conditions with a single comparitor, e.g. a IN (1), to
        an Equals condition (or Not Equals). Equals conditions can be
        pushed into more places than IN conditions.
        """

        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            node.condition = _rewriter(node.condition)
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: HeuristicOptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
