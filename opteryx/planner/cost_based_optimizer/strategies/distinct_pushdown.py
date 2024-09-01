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
Optimization Rule - Distinct Pushdown

Type: Heuristic
Goal: Reduce Rows

Rules:
    - DISTINCT ON can't get pushed
"""

from orso.tools import random_string

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

PASSABLE_AGGREGATIONS = ("MIN", "MAX")
"""
Aggregations we can push the DISTINCT past
"""


class DistinctPushdownStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """ """
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if (node.node_type == LogicalPlanStepType.Distinct) and node.on is None:
            node.nid = context.node_id
            node.plan_path = context.optimized_plan.trace_to_root(context.node_id)
            context.collected_distincts.append(node)
            context.optimized_plan.remove_node(context.node_id, heal=True)
            return context

        if (
            node.node_type == LogicalPlanStepType.Join
            and context.collected_distincts
            and node.type == "cross join"
            and node.unnest_target is not None
        ):
            node.distinct = True
            context.optimized_plan[context.node_id] = node
            context.collected_distincts.clear()
            return context

        if node.node_type in (
            LogicalPlanStepType.Join,
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.AggregateAndGroup,
            LogicalPlanStepType.Aggregate,
        ):
            # anything we couldn't push, we need to put back
            for distinct in context.collected_distincts:
                for nid in distinct.plan_path:
                    if nid in context.optimized_plan:
                        context.optimized_plan.insert_node_before(distinct.nid, distinct, nid)
                        break
            return context

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
