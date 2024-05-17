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
   │  Rewriter │                               │
   └─────┬─────┘                               │
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐                         ╔═══════════╗
   │ AST       │                         ║Cost-Based ║
   │ Rewriter  │                         ║ Optimizer ║
   └─────┬─────┘                         ╚═════▲═════╝
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ Logical   │ Plan │ Heuristic │ Plan │           │
   │   Planner ├──────► Optimizer ├──────► Binder    │
   └───────────┘      └───────────┘      └─────▲─────┘
                                               │Schemas
                                         ┌─────┴─────┐
                                         │           │
                                         │ Catalogue │
                                         └───────────┘
~~~
This is written as a Visitor, unlike the binder which is working from the scanners up to
the projection, this starts at the projection and works toward the scanners. This works well because
the main activity we're doing is splitting nodes, individual node rewrites, and push downs.
"""


from opteryx.planner.cost_based_optimizer.strategies import *
from opteryx.planner.logical_planner import LogicalPlan

from .strategies.optimization_strategy import OptimizerContext

__all__ = "do_cost_based_optimizer"


class CostBasedOptimizerVisitor:
    def __init__(self):
        self.strategies = [
            ConstantFoldingStrategy(),
            BooleanSimplificationStrategy(),
            SplitConjunctivePredicatesStrategy(),
            PredicatePushdownStrategy(),
            ProjectionPushdownStrategy(),
            OperatorFusionStrategy(),
        ]

    def traverse(self, plan: LogicalPlan, strategy) -> LogicalPlan:
        """
        Traverse the logical plan tree and apply optimizations.

        Args:
            tree: The logical plan tree to optimize.

        Returns:
            The optimized logical plan tree.
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
        optimized_plan = strategy.complete(context.optimized_plan, context)
        return optimized_plan

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        current_plan = plan
        for strategy in self.strategies:
            current_plan = self.traverse(current_plan, strategy)
        # DEBUG: log ("AFTER COST OPTIMIZATION")
        # DEBUG: log (current_plan.draw())
        return current_plan


def do_cost_based_optimizer(plan: LogicalPlan) -> LogicalPlan:
    optimizer = CostBasedOptimizerVisitor()
    return optimizer.optimize(plan)
