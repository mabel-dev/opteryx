# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType


def get_nodes_of_type_from_logical_plan(plan: LogicalPlan, types: Tuple[LogicalPlanStepType]):
    matches = []
    for node in plan.nodes(True):
        if node[1].node_type in types:
            matches.append(node)
    return matches


class OptimizerContext:
    """Context object to carry state"""

    def __init__(self, tree: LogicalPlan):
        self.node_id = None
        self.parent_nid = None
        self.last_nid = None
        self.pre_optimized_tree = tree
        self.optimized_plan = LogicalPlan()

        self.seen_projections: int = 0
        self.seen_unions: int = 0
        self.seen_distincts: int = 0
        self.seen_projects_since_distinct: int = 0

        self.collected_predicates: list = []
        """We collect predicates we should be able to push to reads and joins"""

        self.collected_identities: set = set()
        """We collect column identities so we can push column selection as close to the read as possible, including off to remote systems"""

        self.collected_distincts: list = []
        """We collect distincts to try to eliminate rows earlier"""

        self.collected_limits: list = []
        """We collect limits to to to eliminate rows earlier"""

        self.distincted_indentities: set = set()
        """The columns that implicitly exist in the plan because of a distinct"""


class OptimizationStrategy:
    def __init__(self, statistics):
        self.statistics = statistics

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        raise NotImplementedError(
            "Visit method must be implemented in OptimizationStrategy classes."
        )

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        raise NotImplementedError(
            "Complete method must be implemented in OptimizationStrategy classes."
        )

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return True
