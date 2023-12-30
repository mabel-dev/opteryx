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
The Execution Tree is the Graph which defines a Query Plan.

The execution tree contains functionality to:

- build and define the plan
- execute the plan
- manipulate the plan

"""

from typing import Any
from typing import Generator
from typing import Tuple
from typing import Union

import pyarrow

from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.third_party.travers import Graph


class ExecutionTree(Graph):
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.
    """

    def execute(
        self,
    ) -> Generator[Tuple[Union[pyarrow.Table, Any], ResultType], None, None]:
        """
        Implements a 'pull' model execution engine, pulling records starting from
        the last stage (head) of the query plan, and working backwards towards the first stage.

        Yields:
            tuple: The first element is the result (either tabular data or a
                NonTabularResult object). The second element is a ResultType enum,
                indicating the type of the result.
        """
        from opteryx.models import NonTabularResult
        from opteryx.operators import ExplainNode

        def map_operators_to_producers(nodes: list) -> None:
            """
            Walks through the query plan, linking each operator node with its data producers.

            Parameters:
                nodes: list
                    List of operator nodes in the query plan.
            """
            for node in nodes:
                producers = self.ingoing_edges(node)
                operator = self[node]

                if producers:
                    operator.set_producers([self[src_node[0]] for src_node in producers])
                    map_operators_to_producers([src_node[0] for src_node in producers])

        # Validate query plan to ensure it's acyclic
        if not self.is_acyclic():
            raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

        # Retrieve the tail of the query plan, which should ideally be a single head node
        head_nodes = list(set(self.get_exit_points()))

        if len(head_nodes) != 1:
            raise InvalidInternalStateError(
                f"Query plan has {len(head_nodes)} heads, expected exactly 1."
            )

        head_node = head_nodes[0]

        # Special case handling for 'Explain' queries
        if isinstance(self[head_node], ExplainNode):
            yield self.explain(), ResultType.TABULAR
            return

        # Link operators with their producers
        map_operators_to_producers([head_node])

        # Execute the head node's operation
        operator = self[head_node]
        results = operator.execute()

        # If the results are non-tabular, handle them accordingly
        if isinstance(results, NonTabularResult):
            yield results, ResultType.NON_TABULAR
        else:
            yield results, ResultType.TABULAR

    def explain(self):
        from opteryx import operators

        def _inner_explain(node, depth):
            incoming_operators = self.ingoing_edges(node)
            for operator_name in incoming_operators:
                operator = self[operator_name[0]]
                if isinstance(
                    operator, (operators.ExitNode, operators.ExplainNode)
                ):  # Skip ExitNode
                    yield from _inner_explain(operator_name[0], depth)
                    continue
                elif isinstance(operator, operators.BasePlanNode):
                    yield {
                        "operator": operator.name,
                        "config": operator.config,
                        "depth": depth,
                    }
                    yield from _inner_explain(operator_name[0], depth + 1)

        head = list(dict.fromkeys(self.get_exit_points()))
        if len(head) != 1:  # pragma: no cover
            raise InvalidInternalStateError(f"Problem with the plan - it has {len(head)} heads.")
        plan = list(_inner_explain(head[0], 1))

        table = pyarrow.Table.from_pylist(plan)

        yield table
