from opteryx.components.logical_planner import LogicalPlan
from opteryx.components.logical_planner import LogicalPlanNode


# Context object to carry state
class HeuristicOptimizerContext:
    def __init__(self, tree: LogicalPlan):
        self.node_id = None
        self.parent_nid = None
        self.last_nid = None
        self.pre_optimized_tree = tree
        self.optimized_plan = LogicalPlan()

        # We collect predicates we should be able to push to reads and joins
        self.collected_predicates = []

        # We collect column identities so we can push column selection as close to the
        # read as possible, including off to remote systems
        self.collected_identities = set()


class OptimizationStrategy:
    def visit(
        self, node: LogicalPlanNode, context: HeuristicOptimizerContext
    ) -> HeuristicOptimizerContext:
        raise NotImplementedError(
            "Visit method must be implemented in OptimizationStrategy classes."
        )

    def complete(self, plan: LogicalPlan, context: HeuristicOptimizerContext) -> LogicalPlan:
        raise NotImplementedError(
            "Complete method must be implemented in OptimizationStrategy classes."
        )
