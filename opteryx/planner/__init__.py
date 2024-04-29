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
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Heuristic │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘
~~~
"""
import time
from typing import Dict
from typing import Iterable
from typing import Union

from opteryx import config

PROFILE_LOCATION = config.PROFILE_LOCATION


def query_planner(
    operation: str, parameters: Union[Iterable, Dict, None], connection, qid: str, statistics
):
    import orjson

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

    # SQL Rewriter extracts temporal filters
    start = time.monotonic_ns()
    clean_sql, temporal_filters = do_sql_rewrite(operation)
    statistics.time_planning_sql_rewriter += time.monotonic_ns() - start

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
    start = time.monotonic_ns()
    parsed_statements = do_ast_rewriter(
        parsed_statements,
        temporal_filters=temporal_filters,
        parameters=params,
        connection=connection,
    )
    statistics.time_planning_ast_rewriter += time.monotonic_ns() - start

    logical_plan: LogicalPlan = None
    ast: dict = {}

    # Logical Planner converts ASTs to logical plans
    for logical_plan, ast, ctes in do_logical_planning_phase(parsed_statements):  # type: ignore
        # check user has permission for this query type
        query_type = next(iter(ast))
        if query_type not in connection.permissions:
            from opteryx.exceptions import PermissionsError

            raise PermissionsError(
                f"User does not have permission to execute '{query_type}' queries."
            )

        profile_content += (
            orjson.dumps(logical_plan.depth_first_search(), option=orjson.OPT_INDENT_2).decode()
            + "\n\n"
        )
        profile_content += logical_plan.draw() + "\n\n"

        # The Binder adds schema information to the logical plan
        start = time.monotonic_ns()
        bound_plan = do_bind_phase(
            logical_plan,
            connection=connection.context,
            qid=qid,
            # common_table_expressions=ctes,
        )
        statistics.time_planning_binder += time.monotonic_ns() - start

        start = time.monotonic_ns()
        optimized_plan = do_cost_based_optimizer(bound_plan)
        statistics.time_planning_optimizer += time.monotonic_ns() - start

        # before we write the new optimizer and execution engine, convert to a V1 plan
        start = time.monotonic_ns()
        query_properties = QueryProperties(qid=qid, variables=connection.context.variables)
        physical_plan = create_physical_plan(optimized_plan, query_properties)
        statistics.time_planning_physical_planner += time.monotonic_ns() - start
        yield physical_plan
