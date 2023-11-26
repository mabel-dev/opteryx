from orso.tools import random_string

from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import Node

from .optimization_strategy import HeuristicOptimizerContext
from .optimization_strategy import OptimizationStrategy

NODE_ORDER = {
    "Eq": 1,
    "NotEq": 1,
    "Gt": 2,
    "GtEq": 2,
    "Lt": 2,
    "LtEq": 2,
    "Like": 4,
    "ILike": 4,
    "NotLike": 4,
    "NotILike": 4,
}


def tag_predicates(nodes):
    """
    Here we add tags to the predicates to assist with optimization.

    Weighting of predicates based on rules, this is mostly useful for situations where
    we do not have statistics to make cost-based decisions. We're going to start with
    arbitrary numbers, we need to find a way to refine these over time. The logic is
    roughly:
        - 35 is something that is expensive (we're running function)
        - 32 is where we're doing a complex comparison
    """

    for node in nodes:
        node.weight = 0
        node.simple = True
        node.relations = set()

        if not node.node_type == NodeType.COMPARISON_OPERATOR:
            node.weight += 35
            node.simple = False
            continue
        node.score = NODE_ORDER.get(node.value, 12)
        if node.left.node_type == NodeType.LITERAL:
            node.weight += 1
        elif node.left.node_type == NodeType.IDENTIFIER:
            node.weight += 3
            node.relations.add(node.left.source)
        else:
            node.weight += 10
            node.simple = False
        if node.right.node_type == NodeType.LITERAL:
            node.weight += 1
        elif node.right.node_type == NodeType.IDENTIFIER:
            node.weight += 3
            node.relations.add(node.right.source)
        else:
            node.weight += 10
            node.simple = False

    return sorted(nodes, key=lambda node: node.weight)


def _unique_nodes(nodes: list) -> list:
    seen_identities = {}

    for node in nodes:
        identity = node.schema_column.identity
        if identity not in seen_identities:
            seen_identities[identity] = node
        else:
            if node.left.schema_column and node.right.schema_column:
                seen_identities[identity] = node

    return list(seen_identities.values())


def _add_condition(existing_condition, new_condition):
    if not existing_condition:
        return new_condition
    _and = Node(node_type=NodeType.AND)
    _and.left = new_condition
    _and.right = existing_condition
    return _and


class PredicateRewriteStrategy(OptimizationStrategy):
    def optimize(self, node, context):
        if node.node_type in (
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.FunctionDataset,
            LogicalPlanStepType.Subquery,
        ):
            # Handle predicates specific to node types
            context = self._handle_predicates(node, context)

        elif node.node_type == LogicalPlanStepType.Filter:
            # rewrite predicates, to favor conjuctions and reduce negations
            # split conjunctions
            nodes = heuristic_optimizer.rule_split_conjunctive_predicates(node)
            # deduplicate the nodes - note this 'randomizes' the order
            nodes = _unique_nodes(nodes)
            # tag & collect the predicates, ones we can't push, leave here
            non_pushed_predicates = []
            for node in heuristic_optimizer.tag_predicates(nodes):
                if node.simple and len(node.relations) > 0:
                    context.collected_predicates.append(node)
                else:
                    non_pushed_predicates.append(node)

            previous = context.previous_node_id
            for predicate_node in non_pushed_predicates:
                predicate_nid = random_string()
                plan_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter, condition=predicate_node
                )
                context.optimized_tree.add_node(predicate_nid, plan_node)
                context.optimized_tree.add_edge(predicate_nid, previous)
                previous = predicate_nid

            return previous, context

        elif node.node_type == LogicalPlanStepType.Join:
            # push predicates which reference multiple relations here

            if node.type == "cross join" and node.unnest_column:
                # if it's a CROSS JOIN UNNEST - don't try to push any further
                previous = parent
                for predicate in context.collected_predicates:
                    predicate_nid = random_string()
                    plan_node = LogicalPlanNode(
                        node_type=LogicalPlanStepType.Filter, condition=predicate
                    )
                    context.optimized_tree.add_node(predicate_nid, plan_node)
                    context.optimized_tree.add_edge(predicate_nid, previous)
                    previous = predicate_nid
                    continue
                context.collected_predicates = []
                parent = previous
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
                        node.on = _add_condition(node.on, predicate)

                        node.left_columns, node.right_columns = extract_join_fields(
                            node.on, node.left_relation_names, node.right_relation_names
                        )
                        mismatches = get_mismatched_condition_column_types(node.on)
                        if mismatches:
                            from opteryx.exceptions import IncompatibleTypesError

                            raise IncompatibleTypesError(**mismatches)
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

        return None, context

    def complete(self, plan, context):
        # anything we couldn't push, we need to put back
        if context.collected_predicates:
            context.collected_predicates.reverse()
            exit_node = context.optimized_tree.get_exit_points()[0]
            for predicate in context.collected_predicates:
                plan_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter, condition=predicate
                )
                context.optimized_tree.insert_node_before(random_string(), plan_node, exit_node)

    def _handle_predicates(self, node, context):
        previous = context.previous_node_id
        remaining_predicates = []
        for predicate in context.collected_predicates:
            if len(predicate.relations) == 1 and predicate.relations.intersection(
                (node.relation, node.alias)
            ):
                predicate_nid = random_string()
                plan_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter, condition=predicate
                )
                context.optimized_tree.add_node(predicate_nid, plan_node)
                context.optimized_tree.add_edge(predicate_nid, previous)
                previous = predicate_nid
                continue
            remaining_predicates.append(predicate)
        context.collected_predicates = remaining_predicates
        context.previous_node_id = previous
        return context
