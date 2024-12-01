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

from queue import Empty
from queue import Queue
from threading import Lock
from threading import Thread
from typing import Any
from typing import Generator
from typing import Optional
from typing import Tuple

import pyarrow

from opteryx import EOS
from opteryx.config import CONCURRENT_WORKERS
from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.third_party.travers import Graph

morsel_lock = Lock()
active_task_lock = Lock()
active_tasks: int = 0


def active_tasks_increment(value: int):
    global active_tasks
    with active_task_lock:
        active_tasks += value


class PhysicalPlan(Graph):
    """
    The execution tree is defined separately from the planner to simplify the
    complex code that is the planner from the tree that describes the plan.
    """

    def depth_first_search_flat(
        self, node: Optional[str] = None, visited: Optional[set] = None
    ) -> list:
        """
        Returns a flat list representing the depth-first traversal of the graph with left/right ordering.
        """
        if node is None:
            node = self.get_exit_points()[0]

        if visited is None:
            visited = set()

        visited.add(node)
        traversal_list = [(node, self[node])]

        # Sort neighbors based on relationship to ensure left, right, then unlabelled order
        neighbors = sorted(self.ingoing_edges(node), key=lambda x: (x[2] == "right", x[2] == ""))

        for neighbor, _, _ in neighbors:
            if neighbor not in visited:
                child_list = self.depth_first_search_flat(neighbor, visited)
                traversal_list.extend(child_list)

        return traversal_list

    def explain(self, analyze: bool) -> Generator[pyarrow.Table, None, None]:
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
            results = self.execute(query_head)
            if results is not None:
                results_generator, _ = next(results, ([], None))
                for temp in results_generator:
                    pass
            del temp

        plan = list(_inner_explain(head[0], 1))

        table = pyarrow.Table.from_pylist(plan)
        return table

    def execute(self, head_node=None) -> Generator[Tuple[Any, ResultType], Any, Any]:
        from opteryx.operators import ExplainNode
        from opteryx.operators import JoinNode
        from opteryx.operators import ReaderNode
        from opteryx.operators import SetVariableNode
        from opteryx.operators import ShowCreateNode
        from opteryx.operators import ShowValueNode

        morsel_accounting = {nid: 0 for nid in self.nodes()}  # Total morsels received by each node
        node_exhaustion = {nid: False for nid in self.nodes()}  # Exhaustion state of each node

        def mark_node_exhausted(node_id):
            """
            Mark a node as exhausted and propagate exhaustion downstream.
            """
            if node_exhaustion[node_id]:
                return  # Node is already marked as exhausted

            node_exhaustion[node_id] = True

            if isinstance(self[node_id], ReaderNode):
                return

            # Notify downstream nodes
            downstream_nodes = self.outgoing_edges(node_id)
            if len(downstream_nodes) > 1:
                raise InvalidInternalStateError("Cannot FORK execution")
            elif len(downstream_nodes) == 1:
                _, downstream_node, _ = downstream_nodes[0]
                # Check if all parents of downstream_node are exhausted
                if all(
                    node_exhaustion[parent] for parent, _, _ in self.ingoing_edges(downstream_node)
                ):
                    work_queue.put((node_id, EOS))  # EOS signals exhaustion
                    active_tasks_increment(+1)
                    morsel_accounting[node_id] += 1

        def update_morsel_accounting(node_id, morsel_count_change: int):
            """
            Updates the morsel accounting for a node and checks for exhaustion.

            Parameters:
                node_id (str): The ID of the node to update.
                morsel_count_change (int): The change in morsel count (+1 for increment, -1 for decrement).

            Returns:
                None
            """
            with morsel_lock:
                morsel_accounting[node_id] += morsel_count_change
                #                print(
                #                    "ACCOUNT",
                #                    node_id,
                #                    morsel_accounting[node_id],
                #                    morsel_count_change,
                #                    self[node_id].name,
                #                )

                if morsel_accounting[node_id] < 0:
                    raise InvalidInternalStateError("Node input and output count in invalid state.")

                # Check if the node is exhausted
                if morsel_accounting[node_id] == 0:  # No more pending morsels for this node
                    # Ensure all parent nodes are exhausted
                    all_parents_exhausted = all(
                        node_exhaustion[parent] for parent, _, _ in self.ingoing_edges(node_id)
                    )
                    if all_parents_exhausted:
                        mark_node_exhausted(node_id)

        if not self.is_acyclic():
            raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

        head_nodes = list(set(self.get_exit_points()))
        if len(head_nodes) != 1:
            raise InvalidInternalStateError(
                f"Query plan has {len(head_nodes)} heads, expected exactly 1."
            )

        if head_node is None:
            head_node = self[head_nodes[0]]

        # Special case handling for 'Explain' queries
        if isinstance(head_node, ExplainNode):
            yield self.explain(head_node.analyze), ResultType.TABULAR

        elif isinstance(head_node, (SetVariableNode, ShowValueNode, ShowCreateNode)):
            yield head_node(None), ResultType.TABULAR

        else:
            # Work queue for worker tasks
            work_queue = Queue()
            # Response queue for results sent back to the engine
            response_queue = Queue()
            num_workers = CONCURRENT_WORKERS
            workers = []

            def worker_process():
                """
                Worker thread: Processes tasks from the work queue and sends results to the response queue.
                """
                while True:
                    task = work_queue.get()
                    if task is None:
                        break

                    node_id, morsel = task
                    operator = self[node_id]
                    results = operator(morsel)

                    for result in results:
                        # Send results back to the response queue
                        response_queue.put((node_id, result))

                    update_morsel_accounting(node_id, -1)

                    work_queue.task_done()

            # Launch worker threads
            for _ in range(num_workers):
                worker = Thread(target=worker_process)
                worker.daemon = True
                worker.start()
                workers.append(worker)

            def inner_execute(plan):
                # Identify pump nodes
                global active_tasks

                # Get all the nodes which push data into the plan We use DFS to order the
                # nodes to ensure left branch is always before the right branch
                pump_nodes = [
                    (nid, node)
                    for nid, node in self.depth_first_search_flat()
                    if isinstance(node, ReaderNode)
                ]

                # Main engine loop processes pump nodes and coordinates work
                for pump_nid, pump_instance in pump_nodes:
                    for morsel in pump_instance(None):
                        # Initial morsels pushed to the work queue determine downstream operators
                        next_nodes = [target for _, target, _ in self.outgoing_edges(pump_nid)]
                        for downstream_node in next_nodes:
                            # DEBUG: log (f"following initial {self[pump_nid].name} triggering {self[downstream_node].name}")
                            # Queue tasks for downstream operators
                            work_queue.put((downstream_node, morsel))
                            active_tasks_increment(+1)
                            update_morsel_accounting(downstream_node, +1)

                    # Pump is exhausted after emitting all morsels
                    mark_node_exhausted(pump_nid)

                # Process results from the response queue
                def should_stop():
                    all_nodes_exhausted = all(node_exhaustion.values())
                    queues_empty = work_queue.empty() and response_queue.empty()
                    all_nodes_inactive = active_tasks <= 0
                    return all_nodes_exhausted and queues_empty and all_nodes_inactive

                while not should_stop():
                    # Wait for results from workers
                    try:
                        node_id, result = response_queue.get(timeout=0.1)
                    except Empty:
                        continue

                    # if a thread threw a error, we get them in the main
                    # thread here, we just reraise the error here
                    if isinstance(result, Exception):
                        raise result

                    # Handle Empty responses
                    if result is None:
                        active_tasks_increment(-1)
                        continue

                    # Determine downstream operators
                    downstream_nodes = [target for _, target, _ in self.outgoing_edges(node_id)]
                    if len(downstream_nodes) == 0:  # Exit node
                        if result is not None:
                            yield result  # Emit the morsel immediately
                        active_tasks_increment(-1)  # Mark the task as completed
                        continue

                    for downstream_node in downstream_nodes:
                        # Queue tasks for downstream operators
                        active_tasks_increment(+1)
                        # DEBUG: log (f"following {self[node_id].name} triggering {self[downstream_node].name}")
                        work_queue.put((downstream_node, result))
                        update_morsel_accounting(downstream_node, +1)

                    # decrement _after_ we've done the work relation to handling the task
                    active_tasks_increment(-1)

                for worker in workers:
                    work_queue.put(None)

                # Wait for all workers to complete
                for worker in workers:
                    worker.join()

            yield inner_execute(self), ResultType.TABULAR
