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

from opteryx.components.binder.binder_visitor import extract_join_fields
from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node

from .optimization_strategy import CostBasedOptimizerContext
from .optimization_strategy import OptimizationStrategy


def _add_condition(existing_condition, new_condition):
    if not existing_condition:
        return new_condition
    _and = Node(node_type=NodeType.AND)
    _and.left = new_condition
    _and.right = existing_condition
    return _and


class PredicatePushdownStrategy(OptimizationStrategy):
    def visit(
        self, node: LogicalPlanNode, context: CostBasedOptimizerContext
    ) -> CostBasedOptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type in (
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.FunctionDataset,
            LogicalPlanStepType.Subquery,
        ):
            # Handle predicates specific to node types
            context = self._handle_predicates(node, context)

            context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))
            if context.last_nid:
                context.optimized_plan.add_edge(context.node_id, context.last_nid)

        elif node.node_type == LogicalPlanStepType.Filter:
            # collect predicates we can probably push
            if len(node.relations) > 0 and not get_all_nodes_of_type(node.condition, (NodeType.AGGREGATOR,)):
                # record where the node was, so we can put it back
                node.nid = context.node_id
                node.plan_path = context.optimized_plan.trace_to_root(context.node_id)
                context.collected_predicates.append(node)
                context.optimized_plan.remove_node(context.node_id, heal=True)

        elif node.node_type == LogicalPlanStepType.Join and context.collected_predicates:
            # push predicates which reference multiple relations here

            if node.type not in ("cross join", "inner"):
                # dump all the predicates
                # IMPROVE: push past LEFT, SEMI and ANTI joins
                for predicate in context.collected_predicates:
                    context.optimized_plan.insert_node_after(
                        predicate.nid, predicate, context.node_id
                    )
                context.collected_predicates = []
            elif node.type == "cross join" and node.unnest_column:
                # if it's a CROSS JOIN UNNEST - don't try to push any further
                # IMPROVE: we should push everything that doesn't reference the unnested column
                for predicate in context.collected_predicates:
                    context.optimized_plan.insert_node_after(
                        predicate.nid, predicate, context.node_id
                    )
                context.collected_predicates = []
            elif node.type in ("cross join",):  # , "inner"):
                # IMPROVE: add predicates to INNER JOIN conditions
                # we may be able to rewrite as an inner join
                remaining_predicates = []
                for predicate in context.collected_predicates:
                    if (
                        len(predicate.relations) == 2
                        and predicate.condition.value == "Eq"
                        and set(node.right_relation_names + node.left_relation_names)
                        == set(predicate.relations)
                    ):
                        node.type = "inner"
                        node.on = _add_condition(node.on, predicate.condition)
                    else:
                        remaining_predicates.append(predicate)

                if node.on:
                    node.left_columns, node.right_columns = extract_join_fields(
                        node.on, node.left_relation_names, node.right_relation_names
                    )
                    node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))
                    context.collected_predicates = remaining_predicates
            for predicate in context.collected_predicates:
                remaining_predicates = []
                for predicate in context.collected_predicates:
                    if (
                        len(predicate.relations) == 2
                        and predicate.condition.value == "Eq"
                        and set(node.right_relation_names + node.left_relation_names)
                        == set(predicate.relations)
                    ):
                        node.condition = _add_condition(node.condition, predicate)
                    else:
                        remaining_predicates.append(predicate)
                context.collected_predicates = remaining_predicates

            context.optimized_plan.add_node(context.node_id, node)
        return context

    def complete(self, plan: LogicalPlan, context: CostBasedOptimizerContext) -> LogicalPlan:
        # anything we couldn't push, we need to put back
        for predicate in context.collected_predicates:
            for nid in predicate.plan_path:
                if nid in context.optimized_plan:
                    context.optimized_plan.insert_node_before(predicate.nid, predicate, nid)
                    break
        return context.optimized_plan

    def _handle_predicates(
        self, node: LogicalPlanNode, context: CostBasedOptimizerContext
    ) -> CostBasedOptimizerContext:
        remaining_predicates = []
        for predicate in context.collected_predicates:
            if len(predicate.relations) == 1 and predicate.relations.intersection(
                (node.relation, node.alias)
            ):
                if node.connector:
                    connector_capabilities = node.connector.__class__.mro()
                    types = set()
                    if predicate.condition.left and predicate.condition.left.schema_column:
                        types.add(predicate.condition.left.schema_column.type)
                    if predicate.condition.right and predicate.condition.right.schema_column:
                        types.add(predicate.condition.right.schema_column.type)
                    if PredicatePushable in connector_capabilities and node.connector.can_push(
                        predicate, types
                    ):
                        if not node.predicates:
                            node.predicates = []
                        node.predicates.append(predicate.condition)
                        continue
                context.optimized_plan.insert_node_after(predicate.nid, predicate, context.node_id)
                continue
            remaining_predicates.append(predicate)
        context.collected_predicates = remaining_predicates
        return context
