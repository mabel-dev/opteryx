from .constant_folding import ConstantFoldingStrategy
from .flatten_plan import FlattenPlanStrategy
from .predicate_pushdown import PredicatePushdownStrategy
from .projection_pushdown import ProjectionPushdownStrategy

__all__ = [
    "ConstantFoldingStrategy",
    "FlattenPlanStrategy",
    "PredicatePushdownStrategy",
    "ProjectionPushdownStrategy",
]


# correlated filtering (if joining on a column with a filter - apply the filter to the other leg)
# join ordering
# filter ordering


def _tag_predicates(nodes):
    from opteryx.managers.expression import NodeType

    NODE_ORDER: dict = {}
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
        while node.node_type == NodeType.NESTED and node.centre:
            node = node.centre

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
