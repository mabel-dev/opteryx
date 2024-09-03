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

"""
~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │ Rewriter  │                               │
   └─────┬─────┘                               │
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │      │ Physical  │
   │ Rewriter  │      │ Catalogue │      │ Planner   │
   └─────┬─────┘      └───────────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │           │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘

~~~

This module implements a cost-based query optimizer using the Visitor pattern. Unlike the binder,
which processes the logical plan from the scanners up to the projection, this optimizer starts at
the projection and traverses down towards the scanners. This top-down approach is effective for
the primary activities involved in optimization, such as splitting nodes, performing individual
node rewrites, and pushing down predicates and projections.

The optimizer applies a series of strategies, each encapsulating a specific optimization rule.
These strategies are applied sequentially, allowing for incremental improvements to the logical plan.

Key Concepts:
- Visitor Pattern: Used to traverse and modify the logical plan.
- Strategies: Encapsulate individual optimization rules, applied either per-node or per-plan.
- Context: Maintains the state during optimization, including the pre-optimized and optimized plans.

The `CostBasedOptimizerVisitor` class orchestrates the optimization process by applying each strategy
in sequence. The `do_cost_based_optimizer` function serves as the entry point for optimizing a logical plan.

Example Usage:
    optimized_plan = do_cost_based_optimizer(logical_plan)

This module aims to enhance query performance through systematic and incremental optimization steps.
"""

from opteryx.config import DISABLE_OPTIMIZER
from opteryx.planner.cost_based_optimizer.strategies import *
from opteryx.planner.logical_planner import LogicalPlan

from .strategies.optimization_strategy import OptimizerContext

__all__ = "do_cost_based_optimizer"


class CostBasedOptimizerVisitor:
    def __init__(self):
        """
        Initialize the CostBasedOptimizerVisitor with a list of optimization strategies.
        Each strategy encapsulates a specific optimization rule.
        """
        self.strategies = [
            ConstantFoldingStrategy(),
            BooleanSimplificationStrategy(),
            SplitConjunctivePredicatesStrategy(),
            PredicateRewriteStrategy(),
            PredicatePushdownStrategy(),
            ProjectionPushdownStrategy(),
            DistinctPushdownStrategy(),
            OperatorFusionStrategy(),
            RedundantOperationsStrategy(),
            ConstantFoldingStrategy(),
        ]

    def traverse(self, plan: LogicalPlan, strategy) -> LogicalPlan:
        """
        Traverse the logical plan tree and apply the given optimization strategy.

        Parameters:
            plan (LogicalPlan): The logical plan to optimize.
            strategy: The optimization strategy to apply.

        Returns:
            LogicalPlan: The optimized logical plan.
        """
        root_nid = plan.get_exit_points().pop()
        context = OptimizerContext(plan)

        def _inner(nid, parent_nid, context):
            node = context.pre_optimized_tree[nid]
            context.node_id = nid
            context.parent_nid = parent_nid
            context = strategy.visit(node, context)

            for child, _, _ in plan.ingoing_edges(nid):
                _inner(child, nid, context)

        _inner(root_nid, None, context)
        # some strategies operate on the entire plan at once, or need to be told
        # there's no more nodes, we handle both with the .complete
        optimized_plan = strategy.complete(context.optimized_plan, context)
        return optimized_plan

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Optimize the logical plan by applying all registered strategies in sequence.

        Parameters:
            plan (LogicalPlan): The logical plan to optimize.

        Returns:
            LogicalPlan: The fully optimized logical plan.
        """
        current_plan = plan
        for strategy in self.strategies:
            current_plan = self.traverse(current_plan, strategy)
        # DEBUG: log ("AFTER COST OPTIMIZATION")
        # DEBUG: log (current_plan.draw())
        return current_plan


def do_cost_based_optimizer(plan: LogicalPlan) -> LogicalPlan:
    """
    Perform cost-based optimization on the given logical plan.

    Parameters:
        plan (LogicalPlan): The logical plan to optimize.

    Returns:
        LogicalPlan: The optimized logical plan.
    """
    if DISABLE_OPTIMIZER:
        print("[OPTERYX] The optimizer has been disabled, 'DISABLE_OPTIMIZER' variable is TRUE.")
        return plan
    optimizer = CostBasedOptimizerVisitor()
    return optimizer.optimize(plan)
