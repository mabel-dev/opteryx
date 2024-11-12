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

import gc
from typing import Any
from typing import Generator
from typing import Optional
from typing import Tuple
from typing import Union

import pyarrow

from opteryx import EOS
from opteryx import config
from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.third_party.travers import Graph


class PhysicalPlan(Graph):
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.
    """

    def execute(self) -> Generator[Tuple[Union[pyarrow.Table, Any], ResultType], None, None]:
        if config.EXPERIMENTAL_EXECUTION_ENGINE:
            return self.push_executor()
        return self.legacy_executor()

    def legacy_executor(
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

                if len(producers) == 1:
                    # If there is only one producer, set it directly
                    operator.set_producers([self[producers[0][0]]])
                elif len(producers) == 2 and hasattr(operator, "_left_relation"):
                    left_producer = None
                    right_producer = None

                    left_relation = operator._left_relation
                    right_relation = operator._right_relation
                    for source, target, relation in producers:
                        for s, t, r in self.breadth_first_search(source, reverse=True) + [
                            (source, target, relation)
                        ]:
                            if set(left_relation).intersection(
                                {
                                    self[s].parameters.get("alias"),
                                    self[s].parameters.get("relation"),
                                }
                            ):
                                left_producer = self[source]
                            elif set(right_relation).intersection(
                                {
                                    self[s].parameters.get("alias"),
                                    self[s].parameters.get("relation"),
                                }
                            ):
                                right_producer = self[source]

                    if left_producer and right_producer:
                        operator.set_producers([left_producer, right_producer])
                    else:
                        # Default to setting producers as in the current method if left and right cannot be determined
                        operator.set_producers([self[src_node[0]] for src_node in producers])
                else:
                    # Handle cases with more than two producers if applicable
                    operator.set_producers([self[src_node[0]] for src_node in producers])

                # Recursively process the producers
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
        gc.disable()
        results = operator.execute()
        gc.enable()

        # If the results are non-tabular, handle them accordingly
        if isinstance(results, NonTabularResult):
            yield results, ResultType.NON_TABULAR
        else:
            yield results, ResultType.TABULAR

    def explain(self) -> Generator[pyarrow.Table, None, None]:
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

    def explainv2(self, analyze: bool) -> Generator[pyarrow.Table, None, None]:
        from opteryx import operatorsv2

        def _inner_explain(node, depth):
            incoming_operators = self.ingoing_edges(node)
            for operator_name in incoming_operators:
                operator = self[operator_name[0]]
                if isinstance(
                    operator, (operatorsv2.ExitNode, operatorsv2.ExplainNode)
                ):  # Skip ExitNode
                    yield from _inner_explain(operator_name[0], depth)
                    continue
                elif isinstance(operator, operatorsv2.BasePlanNode):
                    record = {
                        "tree": depth,
                        "operator": operator.name,
                        "config": operator.config,
                    }
                    if analyze:
                        record["time_ms"] = operator.execution_time / 1e6
                        record["records_in"] = operator.records_in
                        record["records_out"] = operator.records_out
                    yield record
                    yield from _inner_explain(operator_name[0], depth + 1)

        head = list(dict.fromkeys(self.get_exit_points()))
        if len(head) != 1:  # pragma: no cover
            raise InvalidInternalStateError(f"Problem with the plan - it has {len(head)} heads.")

        # for EXPLAIN ANALYZE, we execute the query and report statistics
        if analyze:
            # we don't want the results, just the details from the plan
            temp = None
            head_node = self.get_exit_points()[0]
            query_head, _, _ = self.ingoing_edges(head_node)[0]
            results = self.push_executor(query_head)
            if results is not None:
                results_generator, _ = next(results, ([], None))
                for temp in results_generator:
                    pass
            del temp

        plan = list(_inner_explain(head[0], 1))

        table = pyarrow.Table.from_pylist(plan)

        yield table

    def depth_first_search_flat(
        self, node: Optional[str] = None, visited: Optional[set] = None
    ) -> list:
        """
        Returns a flat list representing the depth-first traversal of the graph with left/right ordering.

        We do this so we always evaluate the left side of a join before the right side. It technically
        doesn't need the entire plan flattened DFS-wise, but this is what we are doing here to achieve
        the outcome we're after.
        """
        if node is None:
            node = self.get_exit_points()[0]

        if visited is None:
            visited = set()

        visited.add(node)

        # Collect this node's information in a flat list format
        traversal_list = [
            (
                node,
                self[node],
            )
        ]

        # Sort neighbors based on relationship to ensure left, right, then unlabelled order
        neighbors = sorted(self.ingoing_edges(node), key=lambda x: (x[2] == "right", x[2] == ""))

        # Traverse each child, prioritizing left, then right, then unlabelled
        for neighbor, _, _ in neighbors:
            if neighbor not in visited:
                child_list = self.depth_first_search_flat(neighbor, visited)
                traversal_list.extend(child_list)

        return traversal_list

    def push_executor(
        self, head_node=None
    ) -> Tuple[Generator[pyarrow.Table, Any, Any], ResultType]:
        from opteryx.operatorsv2 import ExplainNode
        from opteryx.operatorsv2 import JoinNode
        from opteryx.operatorsv2 import ReaderNode
        from opteryx.operatorsv2 import SetVariableNode
        from opteryx.operatorsv2 import ShowCreateNode
        from opteryx.operatorsv2 import ShowValueNode

        # Validate query plan to ensure it's acyclic
        if not self.is_acyclic():
            raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

        # Retrieve the tail of the query plan, which should ideally be a single head node
        head_nodes = list(set(self.get_exit_points()))

        if len(head_nodes) != 1:
            raise InvalidInternalStateError(
                f"Query plan has {len(head_nodes)} heads, expected exactly 1."
            )

        if head_node is None:
            head_node = self[head_nodes[0]]

        # add the left/right labels to the edges coming into the joins
        joins = [(nid, node) for nid, node in self.nodes(True) if isinstance(node, JoinNode)]
        for nid, join in joins:
            for s, t, r in self.breadth_first_search(nid, reverse=True):
                source_relations = self[s].parameters.get("all_relations", set())
                if set(join._left_relation).intersection(source_relations):
                    self.remove_edge(s, t, r)
                    self.add_edge(s, t, "left")
                elif set(join._right_relation).intersection(source_relations):
                    self.remove_edge(s, t, r)
                    self.add_edge(s, t, "right")

        # Special case handling for 'Explain' queries
        if isinstance(head_node, ExplainNode):
            yield self.explainv2(head_node.analyze), ResultType.TABULAR

        # Special case handling for 'Set' queries
        elif isinstance(head_node, SetVariableNode):
            yield head_node(None), ResultType.NON_TABULAR

        elif isinstance(head_node, (ShowValueNode, ShowCreateNode)):
            yield head_node(None), ResultType.TABULAR

        else:

            def inner_execute(plan):
                # Get the pump nodes from the plan and execute them in order
                pump_nodes = [
                    (nid, node)
                    for nid, node in self.depth_first_search_flat()
                    if isinstance(node, ReaderNode)
                ]
                for pump_nid, pump_instance in pump_nodes:
                    for morsel in pump_instance(None):
                        yield from plan.process_node(pump_nid, morsel)

            yield inner_execute(self), ResultType.TABULAR

    def process_node(self, nid, morsel):
        from opteryx.operatorsv2 import ReaderNode

        node = self[nid]

        if isinstance(node, ReaderNode):
            children = (t for s, t, r in self.outgoing_edges(nid))
            for child in children:
                results = self.process_node(child, morsel)
                results = list(results)
                yield from results
        else:
            results = node(morsel)
            if results is None:
                return None
            if not isinstance(results, list):
                results = [results]
            if morsel == EOS and not any(r == EOS for r in results):
                results.append(EOS)
            for result in results:
                if result is not None:
                    children = [t for s, t, r in self.outgoing_edges(nid)]
                    for child in children:
                        yield from self.process_node(child, result)
                    if len(children) == 0 and result != EOS:
                        yield result

    def sensors(self):
        readings = {}
        for nid in self.nodes():
            node = self[nid]
            readings[node.identity] = node.sensors()
        return readings

    def __del__(self):
        pass


#        print(self.sensors())
