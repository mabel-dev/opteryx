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

This is a very specific rule, on a CROSS JOIN UNNEST, if the result
is the only column in a DISTINCT clause, we push the DISTINCT into
the JOIN.

We've written as a Optimization rule rather than in the JOIN code
as it is expected other instances of pushing DISTINCT may be found.

Order:
    This plan must run after the Projection Pushdown
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

"""
Aggregations we can push the DISTINCT past
"""


class DistinctPushdownStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if (node.node_type == LogicalPlanStepType.Distinct) and node.on is None:
            node.nid = context.node_id
            context.collected_distincts.append(node)
            return context

        if (
            node.node_type == LogicalPlanStepType.Join
            and context.collected_distincts
            and node.type == "cross join"
            and node.unnest_target is not None
            and node.pre_update_columns == node.unnest_target.identity
        ):
            node.distinct = True
            context.optimized_plan[context.node_id] = node
            for distict_node in context.collected_distincts:
                context.optimized_plan.remove_node(distict_node.nid, heal=True)
            context.collected_distincts.clear()
            return context

        if node.node_type in (
            LogicalPlanStepType.Join,
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.AggregateAndGroup,
            LogicalPlanStepType.Aggregate,
            LogicalPlanStepType.Subquery,
        ):
            # we don't push past here
            context.collected_distincts.clear()

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
