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

from typing import Set

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


class ProjectionPushdownStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        """
        Optimize the given node by pushing projections down in the plan.

        Args:
            node: The current node in the logical plan to be optimized.
            context: The context carrying the state and information for optimization.

        Returns:
            A tuple containing the potentially modified node and the updated context.
        """
        if node.columns:  # Assumes node.columns is an iterable or None
            collected_columns = self.collect_columns(node)
            context.collected_identities.update(collected_columns)

        if (
            node.node_type
            in (LogicalPlanStepType.Scan, LogicalPlanStepType.Subquery, LogicalPlanStepType.Union)
            and hasattr(node, "schema")
            and hasattr(node.schema, "columns")
        ):
            # Push all of the projections
            node_columns = [
                col for col in node.schema.columns if col.identity in context.collected_identities
            ]
            # Update the node with the pushed columns
            node.columns = node_columns

        context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))
        if context.parent_nid:
            context.optimized_plan.add_edge(context.node_id, context.parent_nid)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def collect_columns(self, node: LogicalPlanNode) -> Set[str]:
        """
        Collect and return the set of column identities from the given node.

        Args:
            node: The node from which to collect column identities.

        Returns:
            A set of column identities.
        """
        identities = set()
        for column in node.columns or []:  # Ensuring that node.columns is iterable
            if column.node_type == NodeType.IDENTIFIER and column.schema_column:
                identities.add(column.schema_column.identity)
            else:
                identities.update(
                    col.schema_column.identity
                    for col in get_all_nodes_of_type(column, (NodeType.IDENTIFIER,))
                    if col.schema_column
                )

        return identities
