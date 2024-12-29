import os
import sys
import threading
from functools import wraps
from queue import Empty
from queue import Queue
from typing import Dict
from typing import Iterable
from typing import Union

from orso.tools import monitor

from opteryx import CacheManager
from opteryx import config
from opteryx import set_cache_manager
from opteryx.managers.cache import MemcachedCache

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))


os.environ["OPTERYX_DEBUG"] = "1"


cache = MemcachedCache()
set_cache_manager(CacheManager(cache_backend=cache))

SENTINEL = object()  # Unique sentinel value to signal completion of input

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

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
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Heuristic │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘
~~~
"""


PROFILE_LOCATION = config.PROFILE_LOCATION


def query_planner(
    operation: str, parameters: Union[Iterable, Dict, None], connection, qid: str, statistics
):
    import orjson

    from opteryx import Connection
    from opteryx.exceptions import SqlError
    from opteryx.models import QueryProperties
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.cost_based_optimizer import do_cost_based_optimizer
    from opteryx.planner.logical_planner import LogicalPlan
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.planner.temporary_physical_planner import create_physical_plan
    from opteryx.third_party import sqloxide

    conn = Connection()

    # SQL Rewriter extracts temporal filters
    clean_sql, temporal_filters = do_sql_rewrite(operation)
    params: Union[list, dict, None] = None
    if parameters is None:
        params = []
    elif isinstance(parameters, dict):
        params = parameters.copy()
    else:
        params = [p for p in parameters or []]

    profile_content = operation + "\n\n"
    # Parser converts the SQL command into an AST
    try:
        parsed_statements = sqloxide.parse_sql(clean_sql, dialect="mysql")
    except ValueError as parser_error:
        raise SqlError(parser_error) from parser_error
    # AST Rewriter adds temporal filters and parameters to the AST
    parsed_statements = do_ast_rewriter(
        parsed_statements,
        temporal_filters=temporal_filters,
        parameters=params,
        connection=conn,
    )

    logical_plan: LogicalPlan = None
    ast: dict = {}

    # Logical Planner converts ASTs to logical plans
    for logical_plan, ast, ctes in do_logical_planning_phase(parsed_statements):  # type: ignore
        # check user has permission for this query type
        query_type = next(iter(ast))

        profile_content += (
            orjson.dumps(logical_plan.depth_first_search(), option=orjson.OPT_INDENT_2).decode()
            + "\n\n"
        )
        profile_content += logical_plan.draw() + "\n\n"

        # The Binder adds schema information to the logical plan
        bound_plan = do_bind_phase(
            logical_plan,
            connection=conn,
            qid=qid,
            # common_table_expressions=ctes,
        )

        optimized_plan = do_cost_based_optimizer(bound_plan, statistics)

        # before we write the new optimizer and execution engine, convert to a V1 plan
        query_properties = QueryProperties(qid=qid, variables=conn.context.variables)
        physical_plan = create_physical_plan(optimized_plan, query_properties)

        # ===================
        # wrap the decorators with the push-based wrapper
        from types import MethodType

        for nid in physical_plan.nodes():
            node = physical_plan[nid]
            node.execute = MethodType(push_based_decorator(node.execute), node)
        # ===================

        yield physical_plan


class detonator:
    def __init__(self, table):
        self._table = table

    def execute(self):
        return [self._table]


def push_based_decorator(func):
    @wraps(func)
    def wrapper(self, morsel, *args, **kwargs):
        self._producers = [detonator(morsel)]  # Set the input morsel
        return func()

    return wrapper


class PlanExecutor:
    def __init__(self, plan, entry_points, num_threads=2):
        self.plan = plan
        self.entry_points = entry_points
        self.task_queue: Queue = Queue()
        self.result_queue: Queue = Queue()
        self.threads = [threading.Thread(target=self.worker) for _ in range(num_threads)]
        self.shutdown_flag = threading.Event()

    def worker(self):
        while not self.shutdown_flag.is_set():
            try:
                node_id, morsel = self.task_queue.get(timeout=0.1)  # Adjust timeout as needed
            except Empty:
                print("*")
                continue

            node = self.plan[node_id]
            print(node)
            results = node.execute(
                morsel=morsel
            )  # Assuming execution returns an iterable of results

            for result in results:
                self.result_queue.put((node_id, result))
                for next_node_id in self.plan.outgoing_edges(node_id):
                    if next_node_id:
                        self.task_queue.put((next_node_id[1], result))

            self.task_queue.task_done()

    def execute_plan(self):
        # Populate the queue with initial tasks
        for entry_point in self.entry_points:
            self.task_queue.put((entry_point, None))

        # Start all threads
        for thread in self.threads:
            thread.start()

        # Wait for all tasks to be processed
        self.task_queue.join()

        # Optional: process results here, or stop threads
        self.shutdown_flag.set()

        # Make sure all threads finish cleanly
        for thread in self.threads:
            thread.join()

        # Optional: handle results
        while not self.result_queue.empty():
            node_id, result = self.result_queue.get()
            yield result


@monitor()
def push_engine(query):
    plan = next(query_planner(query, [], None, "executor"))  # type: ignore

    executor = PlanExecutor(plan, plan.get_entry_points(), 4)

    print(sum(a.num_rows for a in executor.execute_plan()))

    return None


if __name__ == "__main__":
    import opteryx
    from opteryx.connectors import GcpCloudStorageConnector

    opteryx.register_store("mabel_data", GcpCloudStorageConnector)

    SQL = "SELECT following FROM scratch.parquet WHERE following = 10 AND following > 9 AND following < 11 AND following != 77 AND text != 'hash' OR text != 'hash' OR text != 'hash'"
    SQL = "SELECT followers FROM mabel_data.parquet"
    SQL = "SELECT UserID FROM hits WHERE URL LIKE '%google%';"
    SQL = "SELECT URL, REGEXP_REPLACE(URL, b'E', 'G') AS k FROM hits "

    df = push_engine(SQL)
    print(df)
