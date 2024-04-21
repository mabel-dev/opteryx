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

from orso.tools import random_string

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


def _inner_split(node):
    while node.node_type == NodeType.NESTED:
        node = node.centre

    if node.node_type != NodeType.AND:
        return [node]

    # get the left and right filters
    left_nodes = _inner_split(node.left)
    right_nodes = _inner_split(node.right)

    return left_nodes + right_nodes


class SplitConjunctivePredicatesStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
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
                new_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
                new_node.condition = predicate
                new_node.columns = get_all_nodes_of_type(
                    predicate, select_nodes=(NodeType.IDENTIFIER,)
                )

                sources = []
                for col in new_node.columns:
                    if col.source is not None:
                        sources.append(col.source)
                    if col.schema_column is not None:
                        sources.extend(col.schema_column.origin)
                new_node.relations = set(sources)
                new_nodes.append(new_node)
        else:
            new_nodes = [node]

        for i, new_node in enumerate(new_nodes):
            nid = random_string() if (i + 1) < len(new_nodes) else context.node_id
            context.optimized_plan.add_node(nid, LogicalPlanNode(**new_node.properties))
            if context.parent_nid:
                context.optimized_plan.add_edge(nid, context.parent_nid)
            context.parent_nid = nid

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan
