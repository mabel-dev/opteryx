# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This module provides the execution engine for processing physical plans with an event loop.
"""

import concurrent.futures
import logging
from typing import Any
from typing import Callable
from typing import Generator
from typing import Optional
from typing import Tuple

import pyarrow

from opteryx import EOS
from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import PhysicalPlan
from opteryx.models import QueryStatistics

from .serial_engine import explain

# Configure logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExecutionContext:
    """Encapsulates execution state, thread pool, and async task tracking."""

    def __init__(self, num_workers: int = 2):
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=num_workers)
        self.futures = {}  # Track active futures {future: (node_id, join_leg)}

    def submit_task(
        self, node: Callable, morsel: Optional[pyarrow.Table], join_leg: Optional[str], nid: str
    ) -> None:
        """Submit a task for execution in the thread pool.

        Args:
            node: Callable node to execute
            morsel: Data batch to process
            join_leg: Join identifier if this is part of a join
            nid: Unique node identifier in the plan
        """
        future = self.thread_pool.submit(node, morsel, join_leg)
        self.futures[future] = (nid, join_leg)

    def process_async_tasks(self, plan):
        """Process completed async tasks and dispatch results."""
        if not self.futures:
            return

        # Wait for any completed futures without blocking
        done, not_done = concurrent.futures.wait(
            self.futures.keys(), timeout=0, return_when=concurrent.futures.FIRST_COMPLETED
        )

        # If work is still in progress but nothing is complete, indicate that
        if not done and not_done:
            yield None
            return

        # Process all completed futures
        for future in done:
            nid, join_leg = self.futures.pop(future)
            try:
                results = future.result()
                if results is not None:
                    for result in results:
                        if result is not None:
                            for _, child, leg in plan.outgoing_edges(nid):
                                # Only process stateful nodes immediately
                                node = plan[child]
                                if node.is_stateless:
                                    # Queue up stateless nodes
                                    self.submit_task(node, result, leg, child)
                                else:
                                    yield from process_node(plan, child, result, leg, self)

            except Exception as e:
                logger.error(f"Error processing async task for node {nid}: {e}")

    def shutdown(self):
        """Shut down the execution context."""
        self.thread_pool.shutdown(wait=True)


def execute(
    plan: PhysicalPlan, head_node: str = None, statistics: QueryStatistics = None
) -> Tuple[Generator[pyarrow.Table, Any, Any], ResultType]:
    """Main execution entry point."""
    from opteryx.operators import ExplainNode
    from opteryx.operators import SetVariableNode
    from opteryx.operators import ShowCreateNode
    from opteryx.operators import ShowValueNode

    head_nodes = list(set(plan.get_exit_points()))

    if len(head_nodes) != 1:
        raise InvalidInternalStateError(
            f"Query plan has {len(head_nodes)} heads, expected exactly 1."
        )

    if head_node is None:
        head_node = plan[head_nodes[0]]

    # Special case handling for explain/show/set queries
    if isinstance(head_node, ExplainNode):
        return explain(
            plan, analyze=head_node.analyze, _format=head_node.format
        ), ResultType.TABULAR

    if isinstance(head_node, SetVariableNode):
        return head_node(None), ResultType.NON_TABULAR

    if isinstance(head_node, (ShowValueNode, ShowCreateNode)):
        return head_node(None, None), ResultType.TABULAR

    context = ExecutionContext(num_workers=4)

    def inner_execute(plan: PhysicalPlan) -> Generator:
        """Runs the plan in a hybrid execution model (serial + async)."""
        try:
            # Start processing scan nodes first
            pump_nodes = [
                (nid, node) for nid, node in plan.depth_first_search_flat() if node.is_scan
            ]
            for pump_nid, pump_instance in pump_nodes:
                for morsel in pump_instance(None, None):
                    if morsel is not None:
                        yield from process_node(plan, pump_nid, morsel, None, context)
                yield from process_node(plan, pump_nid, EOS, None, context)

            # Process async tasks until completion
            yield from context.process_async_tasks(plan)

        finally:
            context.shutdown()

    return inner_execute(plan), ResultType.TABULAR


def process_node(
    plan: PhysicalPlan,
    nid: str,
    morsel: pyarrow.Table,
    join_leg: Optional[str],
    context: ExecutionContext,
) -> Generator:
    """Processes a node in the execution plan."""
    node = plan[nid]

    if node.is_scan:
        # Process scan nodes normally
        for _, child, leg in plan.outgoing_edges(nid):
            results = process_node(plan, child, morsel, leg, context)
            yield from (result for result in results if result is not None)
    else:
        if node.is_stateless:
            # Submit stateless nodes for parallel processing
            context.submit_task(node, morsel, join_leg, nid)
            # Process any completed tasks
            yield from context.process_async_tasks(plan)
        else:
            # Process stateful nodes serially
            results = node(morsel, join_leg)
            if results is not None:
                for result in results:
                    if result is not None:
                        children = plan.outgoing_edges(nid)
                        if not children and result != EOS:
                            yield result
                        for _, child, leg in children:
                            yield from process_node(plan, child, result, leg, context)


def event_loop(plan: PhysicalPlan) -> Generator:
    """Event loop to process the plan and async tasks."""
    context = ExecutionContext(num_workers=4)
    try:
        print("loop")
        yield from process_node(plan, plan.get_entry_point(), None, None, context)
        yield from context.process_async_tasks(plan)
    finally:
        context.shutdown()
