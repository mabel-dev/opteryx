from orso.tools import random_string

from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node

from .optimization_strategy import HeuristicOptimizerContext
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
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> HeuristicOptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree

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
            if node.simple and len(node.relations) > 0:
                context.collected_predicates.append(node)
                context.optimized_plan.remove_node(context.node_id, heal=True)

        elif node.node_type == LogicalPlanStepType.Join:
            # push predicates which reference multiple relations here

            if node.type == "cross join" and node.unnest_column:
                # if it's a CROSS JOIN UNNEST - don't try to push any further
                # IMPROVE: we should push everything that doesn't reference the unnested column
                context = self._handle_predicates(node, context)
            elif node.type == "cross join":
                # we may be able to rewrite as an inner join
                remaining_predicates = []
                for predicate in context.collected_predicates:
                    if len(predicate.relations) == 2 and set(
                        node.right_relation_names + node.left_relation_names
                    ) == set(predicate.relations):
                        from opteryx.components.binder.binder_visitor import extract_join_fields
                        from opteryx.components.binder.binder_visitor import (
                            get_mismatched_condition_column_types,
                        )

                        node.type = "inner"
                        node.on = _add_condition(node.on, predicate.condition)

                        node.left_columns, node.right_columns = extract_join_fields(
                            node.on, node.left_relation_names, node.right_relation_names
                        )
                        mismatches = get_mismatched_condition_column_types(node.on)
                        if mismatches:
                            from opteryx.exceptions import IncompatibleTypesError

                            raise IncompatibleTypesError(**mismatches)
                        node.columns = get_all_nodes_of_type(node.on, (NodeType.IDENTIFIER,))
                    else:
                        remaining_predicates.append(predicate)
                context.collected_predicates = remaining_predicates

            for predicate in context.collected_predicates:
                remaining_predicates = []
                for predicate in context.collected_predicates:
                    if len(predicate.relations) == 2 and set(
                        node.right_relation_names + node.left_relation_names
                    ) == set(predicate.relations):
                        node.condition = _add_condition(node.condition, predicate)
                    else:
                        remaining_predicates.append(predicate)
                context.collected_predicates = remaining_predicates

        # DEBUG: log (context.optimized_plan.draw())
        return context

    def complete(self, plan: LogicalPlan, context: HeuristicOptimizerContext) -> LogicalPlan:
        # anything we couldn't push, we need to put back
        if context.collected_predicates:
            context.collected_predicates.reverse()
            exit_node_id = context.optimized_plan.get_exit_points()[0]
            for predicate in context.collected_predicates:
                context.optimized_plan.insert_node_before(random_string(), predicate, exit_node_id)
        return context.optimized_plan

    def _handle_predicates(
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> HeuristicOptimizerContext:
        remaining_predicates = []
        for predicate in context.collected_predicates:
            if len(predicate.relations) == 1 and predicate.relations.intersection(
                (node.relation, node.alias)
            ):
                context.optimized_plan.insert_node_before(
                    random_string(), predicate, context.node_id
                )
                continue
            remaining_predicates.append(predicate)
        context.collected_predicates = remaining_predicates
        return context
