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
Some operators have can be fused to be faster.

Initially we fused Limit and Order operators, this allows us to use a heap sort
algorithm (basically we dicard records we know aren't going to be kept early)
"""


import numpy
from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.operators import HeapSortNode
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.virtual_datasets import no_table_data

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class OperatorFusionStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Order:
            edges = context.optimized_plan.outgoing_edges(context.node_id)
            if len(edges) == 1:
                next_node_id = edges[0][1]
                next_node = context.optimized_plan[next_node_id]
                if next_node.node_type == LogicalPlanStepType.Limit and not next_node.offset:
                    new_node = LogicalPlanNode(node_type=LogicalPlanStepType.HeapSort)
                    new_node.limit = next_node.limit
                    new_node.order_by = node.order_by
                    context.optimized_plan[next_node_id] = new_node
                    context.optimized_plan.remove_node(context.node_id, heal=True)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:

        return plan
