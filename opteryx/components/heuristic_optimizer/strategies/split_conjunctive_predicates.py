from typing import Tuple

from opteryx.components.logical_planner import LogicalPlanNode
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type

from .optimization_strategy import HeuristicOptimizerContext
from .optimization_strategy import OptimizationStrategy


class SplitConjunctivePredicatesStrategy(OptimizationStrategy):
    def optimize(
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> Tuple[LogicalPlanNode, HeuristicOptimizerContext]:
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

        def _inner_split(node):
            if node.node_type != NodeType.AND:
                return [node]

            # get the left and right filters
            left_nodes = _inner_split(node.left)
            right_nodes = _inner_split(node.right)

            return left_nodes + right_nodes

        return _inner_split(node.condition)
