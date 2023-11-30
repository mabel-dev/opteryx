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
         │SQL                                  │Plan
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │Stats │Cost-Based │
   │ Rewriter  │      │ Catalogue ├──────► Optimizer │
   └─────┬─────┘      └─────┬─────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ╔═══════════╗
   │ Logical   │ Plan │           │ Plan ║ Heuristic ║
   │   Planner ├──────► Binder    ├──────► Optimizer ║
   └───────────┘      └───────────┘      ╚═══════════╝
~~~

The plan rewriter does basic heuristic rewrites of the plan, this is an evolution of the old optimizer.

Do things like:
- split predicates into as many AND conditions as possible
- push predicates close to the reads
- push projections close to the reads
- reduce negations

New things:
- replace subqueries with joins

This is written as a Visitor, unlike the binder which is working from the scanners up to
the projection, this starts at the projection and works toward the scanners. This works well because
the main activity we're doing is splitting nodes, individual node rewrites, and push downs.
"""


from opteryx.components.logical_planner import LogicalPlan

from .strategies.optimization_strategy import HeuristicOptimizerContext

__all__ = "do_heuristic_optimizer"


class HeuristicOptimizerVisitor:
    def __init__(self):
        from .strategies import PredicatePushdownStrategy
        from .strategies import ProjectionPushdownStrategy
        from .strategies import SplitConjunctivePredicatesStrategy

        self.strategies = [
            SplitConjunctivePredicatesStrategy(),
            # PredicatePushdownStrategy(),
            ProjectionPushdownStrategy(),
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
        context = HeuristicOptimizerContext(plan)

        def _inner(nid, parent_nid, context):
            node = context.pre_optimized_tree[nid]
            context.node_id = nid
            context.parent_nid = parent_nid
            context = strategy.visit(node, context)

            for child, _, _ in plan.ingoing_edges(nid):
                _inner(child, nid, context)

        _inner(root_nid, None, context)
        optimized_plan = strategy.complete(context.optimized_plan, context)
        # DEBUG: log (optimized_plan.draw())
        return optimized_plan

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        current_plan = plan
        for strategy in self.strategies:
            current_plan = self.traverse(current_plan, strategy)
        return current_plan


def do_heuristic_optimizer(plan: LogicalPlan) -> LogicalPlan:
    optimizer = HeuristicOptimizerVisitor()
    return optimizer.optimize(plan)
