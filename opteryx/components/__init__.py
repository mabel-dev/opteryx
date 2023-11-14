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

from opteryx import config

PROFILE_LOCATION = config.PROFILE_LOCATION


def query_planner(operation: str, parameters: list, connection, qid: str):
    import orjson

    from opteryx.components.ast_rewriter import do_ast_rewriter
    from opteryx.components.binder import do_bind_phase
    from opteryx.components.heuristic_optimizer import do_heuristic_optimizer
    from opteryx.components.logical_planner import do_logical_planning_phase
    from opteryx.components.sql_rewriter import do_sql_rewrite
    from opteryx.components.temporary_physical_planner import create_physical_plan
    from opteryx.exceptions import SqlError
    from opteryx.models import QueryProperties
    from opteryx.third_party import sqloxide

    if isinstance(operation, bytes):
        operation = operation.decode()

    # SQL Rewriter removes whitespace and comments, and extracts temporal filters
    clean_sql, temporal_filters = do_sql_rewrite(operation)
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
        paramters=params,
        connection=connection,
    )
    # Logical Planner converts ASTs to logical plans
    for logical_plan, ast, ctes in do_logical_planning_phase(parsed_statements):
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
        bound_plan = do_bind_phase(
            logical_plan,
            connection=connection.context,
            qid=qid,
            # common_table_expressions=ctes,
        )

        heuristic_optimized_plan = do_heuristic_optimizer(bound_plan)

        # before we write the new optimizer and execution engine, convert to a V1 plan
        query_properties = QueryProperties(qid=qid, variables=connection.context.variables)
        physical_plan = create_physical_plan(heuristic_optimized_plan, query_properties)
        yield physical_plan
