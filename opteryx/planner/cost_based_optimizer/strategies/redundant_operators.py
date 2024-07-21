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
This optimization runs toward the end of the set, it removes operators which
were useful during planning and optimization.

- Some projections are redundant (reselecting down to the columns which the
  providing operation has already limited down to).
- SubQuery nodes are useful for planning and optimization, but don't do
  anything during execution, we can remove them here.

Both of these operations are cheap to execute, the benefit for this
optimization isn't expected to be realized until we implement multiprocessing
and there is work associated with IPC which we are avoiding by removing
impotent steps.
"""

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class RedundantOperationsStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        # If we're a project and the providing step has the same columns, we're
        # not doing anything so can be removed.
        if node.node_type == LogicalPlanStepType.Project:
            providers = context.pre_optimized_tree.ingoing_edges(context.node_id)
            if len(providers) == 1:
                provider_node = context.pre_optimized_tree[providers[0][0]]
                provider_columns = {c.schema_column.identity for c in provider_node.columns}
                my_columns = {c.schema_column.identity for c in node.columns}
                if provider_columns == my_columns:
                    context.optimized_plan.remove_node(context.node_id, heal=True)

        # Subqueries are useful for planning but not needed for execution
        if node.node_type == LogicalPlanStepType.Subquery:
            context.optimized_plan.remove_node(context.node_id, heal=True)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
