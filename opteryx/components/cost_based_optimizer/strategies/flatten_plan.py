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
Flatten Plan

This isn't strictly cost-based, but needs to happen between two cost-based
strategies - predicate pushdown & cost-based ordering.

The purpose of this strategy is to group predicates under a single AND or
OR, this then enables cost-based ordering of predicates. Once we have a
group of predicates under a single AND (or OR), we can order them so that
we balance the cost of performing the filter and the selectivity of the
filter. We can do this without flattening, but it will take more steps to
execute as we need to traverse the DAG to get the predicates.
"""

from orso.tools import random_string

from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type

from .optimization_strategy import CostBasedOptimizerContext
from .optimization_strategy import OptimizationStrategy


class FlattenPlanStrategy(OptimizationStrategy):
    def visit(
        self, node: LogicalPlanNode, context: CostBasedOptimizerContext
    ) -> CostBasedOptimizerContext:
        """
        Our approach is pretty basic, when we see a filter, start collecting
        until we don't see any more filters and then combine them together into
        a single filter node.
        """

        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            context.collected_predicates.append(node.condition)
            context.optimized_plan.remove_node(context.node_id, heal=True)
        elif len(context.collected_predicates):
            new_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter, junction=NodeType.AND)
            new_node.condition_list = context.collected_predicates
            context.collected_predicates = []
            context.optimized_plan.insert_node_after(random_string(), new_node, context.node_id)

        return context

    def complete(self, plan: LogicalPlan, context: CostBasedOptimizerContext) -> LogicalPlan:
        return plan
