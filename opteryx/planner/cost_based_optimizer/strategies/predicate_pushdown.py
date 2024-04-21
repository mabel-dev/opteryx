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

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.binder.binder_visitor import extract_join_fields
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

IN_REWRITES = {"InList": "Eq", "NotInList": "NotEq"}
LIKE_REWRITES = {"Like": "Eq", "NotList": "NotEq"}


def _add_condition(existing_condition, new_condition):
    if not existing_condition:
        return new_condition
    _and = Node(node_type=NodeType.AND)
    _and.left = new_condition
    _and.right = existing_condition
    return _and


def _rewrite_predicate(predicate):
    """
    Rewrite individual predicates to forms able to push to more places
    """
    if predicate.value in LIKE_REWRITES:
        # LIKE conditions with no wildcards => Eq
        if (
            predicate.right.node_type == NodeType.LITERAL
            and "%" not in predicate.right.value
            and "_" not in predicate.right.value
        ):
            predicate.value = LIKE_REWRITES[predicate.value]
            return predicate
    if predicate.value in IN_REWRITES:
        # IN conditions on single values => Eq
        if predicate.right.node_type == NodeType.LITERAL and len(predicate.right.value) == 1:
            predicate.value = IN_REWRITES[predicate.value]
            predicate.right.value = predicate.right.value.pop()
            predicate.right.type = predicate.right.sub_type
            predicate.right.sub_type = None
            return predicate

    return predicate


def _tag_predicate(predicate):
    """
    Add flags, tags, labels, and notes to predicates
    """

    # predicate.relations = set()
    # add label for if a predicate is a filter or a join
    # add in nominal per-row cost information / time to execute 1 million times

    return predicate


class PredicatePushdownStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type in (LogicalPlanStepType.Scan, LogicalPlanStepType.FunctionDataset):
            # Handle predicates specific to node types
            context = self._handle_predicates(node, context)

            context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))
            if context.last_nid:
                context.optimized_plan.add_edge(context.node_id, context.last_nid)

        elif node.node_type == LogicalPlanStepType.Filter:
            # collect predicates we can probably push
            if (
                len(node.relations) > 0
                and not get_all_nodes_of_type(node.condition, (NodeType.AGGREGATOR,))
                and len(get_all_nodes_of_type(node.condition, (NodeType.IDENTIFIER,))) == 1
            ):
                # record where the node was, so we can put it back
                node.nid = context.node_id
                node.plan_path = context.optimized_plan.trace_to_root(context.node_id)

                node.condition = _rewrite_predicate(node.condition)

                context.collected_predicates.append(node)
                context.optimized_plan.remove_node(context.node_id, heal=True)

        elif node.node_type == LogicalPlanStepType.Join:

            def _inner(node):
                # if we're an AND, check each leg
                if node.node_type == NodeType.AND:
                    # if one of the sides of an AND is a collectable condition,
                    # collect it and replace the AND with the other side only.
                    collected_left, _ = _inner(node.left)
                    collected_right, _ = _inner(node.right)
                    if collected_left:
                        return collected_left, node.right
                    if collected_right:
                        return collected_right, node.left
                    return [], node
                # if we're a predicate, check it, left first
                if len(get_all_nodes_of_type(node.left, (NodeType.IDENTIFIER,))) == 0:
                    return [node], None
                if len(get_all_nodes_of_type(node.right, (NodeType.IDENTIFIER,))) == 0:
                    return [node], None

                return [], node

            if node.on:
                new_predicates, node.on = _inner(node.on)
                context.collected_predicates.extend(
                    LogicalPlanNode(
                        LogicalPlanStepType.Filter,
                        condition=node,
                        nid=random_string(),
                        relations={
                            n.source for n in get_all_nodes_of_type(node, (NodeType.IDENTIFIER,))
                        },
                    )
                    for node in new_predicates
                )

            if context.collected_predicates:
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
                    # don't push filters we can't resolve here though
                    remaining_predicates = []
                    for predicate in context.collected_predicates:

                        known_columns = set(col.schema_column.identity for col in predicate.columns)
                        query_columns = {
                            predicate.condition.left.schema_column.identity,
                            predicate.condition.right.schema_column.identity,
                        }

                        # Here we're pushing filters into the UNNEST - this means that
                        # CROSS JOIN UNNEST will produce fewer rows... it still does
                        # the equality check, but all in one step which is generally faster
                        if (
                            len(predicate.columns) == 1
                            and predicate.columns[0].value == node.unnest_alias
                        ):
                            if predicate.condition.value in {"Eq", "InList"}:
                                filters = node.filters or []
                                new_values = predicate.condition.right.value
                                if not isinstance(new_values, (list, set, tuple)):
                                    new_values = [new_values]
                                else:
                                    new_values = list(new_values)
                                node.filters = set(filters + new_values)

                        elif (
                            query_columns == (known_columns)
                            or node.unnest_target.identity in query_columns
                        ):
                            context.optimized_plan.insert_node_after(
                                predicate.nid, predicate, context.node_id
                            )
                        else:
                            remaining_predicates.append(predicate)
                    context.collected_predicates = remaining_predicates
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

            if node.on is None and node.type == ("inner"):
                raise UnsupportedSyntaxError(
                    "INNER JOIN has no conditions, did you mean to use a CROSS JOIN?"
                )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # anything we couldn't push, we need to put back
        for predicate in context.collected_predicates:
            for nid in predicate.plan_path:
                if nid in context.optimized_plan:
                    context.optimized_plan.insert_node_before(predicate.nid, predicate, nid)
                    break
        return context.optimized_plan

    def _handle_predicates(
        self, node: LogicalPlanNode, context: OptimizerContext
    ) -> OptimizerContext:
        remaining_predicates = []
        for predicate in context.collected_predicates:
            if len(predicate.relations) >= 1 and predicate.relations.intersection(
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
