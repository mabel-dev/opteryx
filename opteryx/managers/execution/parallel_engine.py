import multiprocessing as mp
from queue import Empty
from typing import Any
from typing import Generator
from typing import Tuple

import pyarrow

from opteryx import EOS
from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import PhysicalPlan
from opteryx.models import QueryStatistics

WORKERS = 4
kill = object()


def execute(
    plan: PhysicalPlan, statistics: QueryStatistics = None, num_workers: int = WORKERS
) -> Tuple[Generator[pyarrow.Table, Any, Any], ResultType]:
    """
    Execute the physical plan with morsel-level parallelism.

    Parameters:
        plan: PhysicalPlan
            The physical plan to execute.
        statistics: QueryStatistics, optional
            Object to collect query statistics, defaults to None.
        num_workers: int, optional
            Number of parallel workers for processing morsels, defaults to 4.

    Returns:
        Tuple[Generator[pyarrow.Table, Any, Any], ResultType]
            A generator producing pyarrow tables and the result type.
    """
    try:
        mp.set_start_method("fork", force=True)

        # Ensure there's a single head node
        head_nodes = list(set(plan.get_exit_points()))
        if len(head_nodes) != 1:
            raise InvalidInternalStateError(
                f"Query plan has {len(head_nodes)} heads, expected exactly 1."
            )

        head_node = plan[head_nodes[0]]

        # Queue for incoming morsels and a queue for results
        work_queue = mp.Queue()
        result_queue = mp.Queue()

        # Create a worker pool for processing morsels
        pool = mp.Pool(num_workers, _worker_init, (plan, work_queue, result_queue))

        def inner_execute(plan: PhysicalPlan) -> Generator:
            # Get the pump nodes from the plan and execute them in order
            pump_nodes = [
                (nid, node) for nid, node in plan.depth_first_search_flat() if node.is_scan
            ]
            for pump_nid, pump_instance in pump_nodes:
                work_queue.put((pump_nid, None, None))
                work_queue.put((pump_nid, EOS, None))
            while True:
                try:
                    result = result_queue.get(timeout=0.1)
                    print("got final result", type(result))
                    if result == EOS:
                        continue
                    return result
                except Empty:
                    pass

        result_generator = inner_execute(plan)

        print("I'm done here")

        #        pool.close()
        #        pool.join()

        return result_generator, ResultType.TABULAR

    finally:
        # Close and join the pool after execution
        pass


def _worker_init(plan: PhysicalPlan, work_queue: mp.Queue, completion_queue: mp.Queue):
    """
    Initialize the worker process for morsel-level parallelism.

    Parameters:
        plan: PhysicalPlan
            The overall physical plan.
        morsel_queue: mp.Queue
            Queue from which morsels are fetched.
        result_queue: mp.Queue
            Queue to which processed morsels are pushed.
    """
    while True:
        try:
            work = work_queue.get(timeout=0.1)
        except Empty:
            continue

        nid, morsel, join_leg = work

        operator = plan[nid]

        results = operator(morsel, join_leg)

        if results is None:
            continue

        print("Worker got work for", operator.name, type(morsel), "results")

        for result in (result for result in results if result is not None):
            children = plan.outgoing_edges(nid)
            print("results", type(result), children)
            if len(children) == 0:
                print("done")
                completion_queue.put(result)
            for _, child, leg in children:
                work_queue.put((child, result, leg))
