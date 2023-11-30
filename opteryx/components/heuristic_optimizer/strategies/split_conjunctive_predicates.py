from orso.tools import random_string

from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type

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


def _tag_predicates(nodes):
    """
    Here we add tags to the predicates to assist with optimization.

    Weighting of predicates based on naive rules, this is mostly useful for situations where
    we do not have statistics to make cost-based decisions later. We're going to start with
    arbitrary numbers, we need to find a way to refine these over time. The logic is
    roughly:
        - 35 is something that is expensive (we're running function)
        - 32 is where we're doing a complex comparison
        ...
        - 7 are IS/IS NOT filters
        - 5 is doing an eqi comparison on a column and a literal
        - 3 is doing an eqi comparison on two literals (don't actually do that in a filter)
    """

    for node in nodes:
        node.weight = 0
        node.simple = True
        node.relations = set()

        if node.condition.node_type == NodeType.UNARY_OPERATOR:
            # these are IS/IS NOT filters
            node.weight += 7
            node.relations.add(node.condition.centre.source)
            continue
        if not node.condition.node_type == NodeType.COMPARISON_OPERATOR:
            node.weight += 35
            node.simple = False
            continue
        node.weight = NODE_ORDER.get(node.condition.value, 12)
        if node.condition.left.node_type == NodeType.LITERAL:
            node.weight += 1
        elif node.condition.left.node_type == NodeType.IDENTIFIER:
            node.weight += 3
            node.relations.add(node.condition.left.source)
        else:
            node.weight += 10
            node.simple = False
        if node.condition.right.node_type == NodeType.LITERAL:
            node.weight += 1
        elif node.condition.right.node_type == NodeType.IDENTIFIER:
            node.weight += 3
            node.relations.add(node.condition.right.source)
        else:
            node.weight += 10
            node.simple = False

    return sorted(nodes, key=lambda node: node.weight, reverse=True)


def _inner_split(node):
    if node.node_type != NodeType.AND:
        return [node]

    # get the left and right filters
    left_nodes = _inner_split(node.left)
    right_nodes = _inner_split(node.right)

    return left_nodes + right_nodes


def _unique_nodes(nodes: list) -> list:
    seen_identities = {}

    for node in nodes:
        if node.condition:
            identity = node.condition.schema_column.identity
            if identity not in seen_identities:
                # if it's the first time we've seen it, capture it
                seen_identities[identity] = node
            elif node.condition.left.schema_column and node.condition.right.schema_column:
                # if we're seeing it again AND it has left and right columns, it's probably
                # the complete node so replace what we captured before
                seen_identities[identity] = node

    return list(seen_identities.values())


class SplitConjunctivePredicatesStrategy(OptimizationStrategy):
    def visit(
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> HeuristicOptimizerContext:
        """
        Conjunctive Predicates (ANDs) can be split and executed in any order to get the
        same result. This means we can split them into separate steps in the plan.

        The reason for splitting is two-fold:

        1)  Smaller expressions are easier to move around the query plan as they have fewer
            dependencies.
        2)  Executing predicates like this means each runs in turn, filtering out some of
            the records meaning susequent predicates will be operating on fewer records,
            which is generally faster. We can also order these predicates to get a faster
            result, balancing the selectivity (get rid of more records faster) vs cost of
            the check (a numeric check is faster than a string check)
        """
        if node.node_type == LogicalPlanStepType.Filter:
            split_predicates = _inner_split(node.condition)
            new_nodes = []
            for predicate in split_predicates:
                new_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter, condition=predicate
                )
                new_node.columns = get_all_nodes_of_type(
                    node.condition, select_nodes=(NodeType.IDENTIFIER,)
                )
                new_nodes.append(new_node)
            new_nodes = _unique_nodes(new_nodes)
            new_nodes = _tag_predicates(new_nodes)
        else:
            new_nodes = [node]

        for i, new_node in enumerate(new_nodes):
            nid = random_string() if (i + 1) < len(new_nodes) else context.node_id
            context.optimized_plan.add_node(nid, LogicalPlanNode(**new_node.properties))
            if context.parent_nid:
                context.optimized_plan.add_edge(nid, context.parent_nid)
            context.parent_nid = nid

        return context

    def complete(self, plan: LogicalPlan, context: HeuristicOptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
