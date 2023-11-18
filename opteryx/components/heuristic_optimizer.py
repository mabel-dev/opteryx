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
from opteryx.components.logical_planner import LogicalPlanStepType
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type


# Context object to carry state
class HeuristicOptimizerContext:
    def __init__(self, tree: LogicalPlan):
        self.pre_optimized_tree = tree
        self.optimized_tree = LogicalPlan()

        # We collect predicates that reference single relations so we can push them
        # as close to the read as possible, including off to remote systems
        self.collected_predicates = []

        # We collect column identities so we can push column selection as close to the
        # read as possible, including off to remote systems
        self.collected_identities = set()


# Optimizer Visitor
class HeuristicOptimizerVisitor:
    def rewrite_predicates(self, node):
        pass

    def collect_columns(self, node):
        if node.columns:
            return {
                col.schema_column.identity
                for column in node.columns
                for col in (
                    [column]
                    if column.node_type == NodeType.IDENTIFIER
                    else get_all_nodes_of_type(column, (NodeType.IDENTIFIER,))
                )
                if col.schema_column
            }
        return set()

    def visit(self, parent: str, nid: str, context: HeuristicOptimizerContext):
        # collect column references to push PROJECTION
        # rewrite conditions to get as many AND conditions as possible
        # collect predicates which reference one relation to push SELECTIONS
        # get rid of NESTED nodes

        node = context.pre_optimized_tree[nid]

        # do this before any transformations
        if node.node_type != LogicalPlanStepType.Scan:
            context.collected_identities = context.collected_identities.union(
                self.collect_columns(node)
            )

        if node.node_type == LogicalPlanStepType.Filter:
            # rewrite predicates, to favor conjuctions and reduce negations
            # split conjunctions
            # collect predicates
            pass
        if node.node_type == LogicalPlanStepType.Scan:
            # push projections
            node_columns = [
                col for col in node.schema.columns if col.identity in context.collected_identities
            ]
            # these are the pushed columns
            node.columns = node_columns
        if node.node_type == LogicalPlanStepType.Join:
            # push predicates which reference multiple relations here
            pass

        context.optimized_tree.add_node(nid, node)
        if parent:
            context.optimized_tree.add_edge(nid, parent)

        return context

    def traverse(self, tree: LogicalPlan):
        root = tree.get_exit_points().pop()
        context = HeuristicOptimizerContext(tree)

        def _inner(parent, node, context):
            context = self.visit(parent, node, context)
            for child, _, _ in tree.ingoing_edges(node):
                _inner(node, child, context)

        _inner(None, root, context)
        return context.optimized_tree


def do_heuristic_optimizer(plan: LogicalPlan) -> LogicalPlan:
    optimizer = HeuristicOptimizerVisitor()
    return optimizer.traverse(plan)
