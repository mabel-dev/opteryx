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

CONCURRENT_WORKERS = 1


class EOSHandlerMixin:
    def __init__(self, work_queue):
        self.node_exhaustion = {}  # Tracks which nodes are exhausted
        self.morsel_accounting = {}  # Tracks active morsels per node/leg
        self.work_queue = work_queue
        self.active_tasks = 0

    def active_tasks_increment(self, value: int):
        with active_task_lock:
            self.active_tasks += value

    def initialize_eos_tracking(self, nodes):
        """
        Initialize EOS tracking and morsel accounting for all nodes.
        """
        from opteryx.operators import JoinNode

        self.node_exhaustion = {
            (nid, None): False for nid, node in nodes if not isinstance(node, JoinNode)
        }
        self.morsel_accounting = {
            (nid, None): 0 for nid, node in nodes if not isinstance(node, JoinNode)
        }

        for join_nid in (nid for nid, node in nodes if isinstance(node, JoinNode)):
            self.node_exhaustion[(join_nid, "left")] = False
            self.node_exhaustion[(join_nid, "right")] = False
            self.morsel_accounting[(join_nid, "left")] = 0
            self.morsel_accounting[(join_nid, "right")] = 0

    def mark_node_exhausted(self, node_id: str, leg: Optional[str] = None):
        """
        Mark a node and leg as exhausted and propagate EOS downstream.
        """
        if self.node_exhaustion[(node_id, leg)]:
            return  # Already marked exhausted

        self.node_exhaustion[(node_id, leg)] = True

        # Propagate EOS to downstream nodes
        self.queue_task(node_id, leg, EOS)

    def update_morsel_accounting(self, node_id: str, leg: Optional[str], delta: int):
        """
        Update morsel accounting for a node and check for exhaustion.
        """
        with morsel_lock:
            self.morsel_accounting[(node_id, leg)] += delta
            if self.morsel_accounting[(node_id, leg)] < 0:
                print(self.morsel_accounting)
                raise InvalidInternalStateError("Morsel accounting is invalid.")

            # If no more morsels, check if all providers are exhausted
            if self.morsel_accounting[(node_id, leg)] == 0:
                self.check_and_mark_exhaustion(node_id, leg)

    def check_and_mark_exhaustion(self, node_id: str, leg: Optional[str]):
        """
        Check if all upstream providers for a node are exhausted.
        """
        for provider, _, provider_leg in self.ingoing_edges(node_id):
            if not self.node_exhaustion.get((provider, provider_leg), False):
                return  # A provider is still active

        self.mark_node_exhausted(node_id, leg)

    def queue_task(self, node_id: str, leg: Optional[str], payload: Any):
        """
        Queue a task for a worker.
        """
        print(
            "WORK PUT",
            node_id,
            leg,
            "Table" if isinstance(payload, pyarrow.Table) else "EOS" if payload == EOS else payload,
            flush=True,
        )
        self.work_queue.put((node_id, leg, payload))
        self.active_tasks_increment(+1)
        self.morsel_accounting[(node_id, leg)] += 1

    # Process results from the response queue
    def work_complete(self) -> bool:
        all_nodes_exhausted = all(self.node_exhaustion.values())
        no_active_tasks = self.active_tasks <= 0
        return all_nodes_exhausted and no_active_tasks


class PhysicalPlan(Graph, EOSHandlerMixin):
    """
    The execution tree is defined separately from the planner to simplify the
    complex code that is the planner from the tree that describes the plan.
    """

    def __init__(self):
        # Work queue for worker tasks
        self.work_queue = Queue(maxsize=10)
        # Response queue for results sent back to the engine
        self.response_queue = Queue(maxsize=10)

        Graph.__init__(self)
        EOSHandlerMixin.__init__(self, self.work_queue)

    def depth_first_traversal_with_order(
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
                child_list = self.depth_first_traversal_with_order(neighbor, visited)
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

        if not self.is_acyclic():
            raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

        head_nodes = list(set(self.get_exit_points()))
        if len(head_nodes) != 1:
            raise InvalidInternalStateError(
                f"Query plan has {len(head_nodes)} heads, expected exactly 1."
            )

        if head_node is None:
            head_node = self[head_nodes[0]]

        self.initialize_eos_tracking(self.nodes(True))

        # add the left/right labels to the edges coming into the joins
        joins = ((nid, node) for nid, node in self.nodes(True) if isinstance(node, JoinNode))
        for nid, join in joins:
            for provider, provider_target, provider_relation in self.ingoing_edges(nid):
                reader_edges = {
                    (source, target, relation)
                    for source, target, relation in self.breadth_first_search(
                        provider, reverse=True
                    )
                }  # if hasattr(self[target], "uuid")}
                if hasattr(self[provider], "uuid"):
                    reader_edges.add((provider, provider_target, provider_relation))

                for s, t, r in reader_edges:
                    if self[s].uuid in join.left_readers:
                        self.add_edge(provider, nid, "left")
                    elif self[s].uuid in join.right_readers:
                        self.add_edge(provider, nid, "right")

            tester = self.breadth_first_search(nid, reverse=True)
            if not any(r == "left" for s, t, r in tester):
                raise InvalidInternalStateError("Join has no LEFT leg")
            if not any(r == "right" for s, t, r in tester):
                raise InvalidInternalStateError("Join has no RIGHT leg")

        # Special case handling for 'Explain' queries
        if isinstance(head_node, ExplainNode):
            yield self.explain(head_node.analyze), ResultType.TABULAR

        elif isinstance(head_node, (SetVariableNode, ShowValueNode, ShowCreateNode)):
            yield head_node(None), ResultType.TABULAR

        else:
            num_workers = CONCURRENT_WORKERS
            workers = []

            def worker_process():
                """
                Worker thread: Processes tasks from the work queue and sends results to the response queue.
                """
                while True:
                    task = self.work_queue.get()
                    if task is None:
                        print("WORK GET", task)
                        break

                    print(
                        "WORK GET",
                        task[0],
                        task[1],
                        "Table"
                        if isinstance(task[2], pyarrow.Table)
                        else "EOS"
                        if task[2] == EOS
                        else task[2],
                        self[task[0]].name,
                        flush=True,
                    )
                    node_id, join_leg, morsel = task
                    operator = self[node_id]
                    results = operator(morsel, join_leg)

                    for result in results:
                        # Send results back to the response queue
                        self.response_queue.put((node_id, join_leg, result))

                    self.update_morsel_accounting(node_id, join_leg, -1)

                    self.work_queue.task_done()

            # Launch worker threads
            for _ in range(num_workers):
                worker = Thread(target=worker_process)
                worker.daemon = True
                worker.start()
                workers.append(worker)

            def inner_execute(plan):
                # Get all the nodes which push data into the plan We use DFS to order the
                # nodes to ensure left branch is always before the right branch
                pump_nodes = [
                    (nid, node)
                    for nid, node in self.depth_first_traversal_with_order()
                    if isinstance(node, ReaderNode)
                ]

                # Main engine loop processes pump nodes and coordinates work
                for pump_nid, pump_instance in pump_nodes:
                    for morsel in pump_instance(None, None):
                        # Initial morsels pushed to the work queue determine downstream operators
                        consumer_nodes = [
                            (target, join_leg)
                            for _, target, join_leg in self.outgoing_edges(pump_nid)
                        ]
                        for consumer_node, join_leg in consumer_nodes:
                            # DEBUG: log (f"following initial {self[pump_nid].name} ({pump_nid}) triggering {self[consumer_node].name} ({consumer_node})")
                            # Queue tasks for consumer operators
                            self.queue_task(consumer_node, join_leg, morsel)

                    # Pump is exhausted after emitting all morsels
                    print("pump exhausted", pump_nid)
                    self.update_morsel_accounting(pump_nid, None, 0)

                while not self.work_complete():
                    # Wait for results from workers
                    print(list(self.node_exhaustion.values()), self.active_tasks)
                    try:
                        node_id, join_leg, result = self.response_queue.get(timeout=0.1)
                    except Empty:
                        print(".")
                        continue

                    # if a thread threw a error, we get them in the main
                    # thread here, we just reraise the error here
                    if isinstance(result, Exception):
                        raise result

                    # Handle Empty responses
                    if result is None:
                        self.active_tasks_increment(-1)
                        continue

                    # Determine downstream operators
                    downstream_nodes = [
                        (target, join_leg) for _, target, join_leg in self.outgoing_edges(node_id)
                    ]
                    if len(downstream_nodes) == 0:  # Exit node
                        if result is not None:
                            yield result  # Emit the morsel immediately
                        self.active_tasks_increment(-1)  # Mark the task as completed
                        continue

                    for downstream_node, join_leg in downstream_nodes:
                        # Queue tasks for downstream operators
                        # DEBUG: log (f"following {self[node_id].name} ({node_id}) triggering {self[downstream_node].name} ({downstream_node})", flush=True)
                        self.queue_task(downstream_node, join_leg, result)

                    # decrement _after_ we've done the work relation to handling the task
                    self.active_tasks_increment(-1)

                for worker in workers:
                    self.work_queue.put(None)

                # Wait for all workers to complete
                for worker in workers:
                    worker.join()

            yield inner_execute(self), ResultType.TABULAR
