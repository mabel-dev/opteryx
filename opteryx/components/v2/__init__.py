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
   │ AST       │      │           │Stats │           │
   │ Rewriter  │      │ Catalogue ├──────► Optimizer │
   └─────┬─────┘      └─────┬─────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Tree      │
   │   Planner ├──────► Binder    ├──────►  Rewriter │
   └───────────┘      └───────────┘      └───────────┘
~~~
"""

from opteryx import config
from opteryx.components.v2.logical_planner import QUERY_BUILDERS

PROFILE_LOCATION = config.PROFILE_LOCATION


def query_planner(operation, parameters, connection):
    import orjson

    from opteryx.components.v2.ast_rewriter import do_ast_rewriter
    from opteryx.components.v2.binder import do_bind_phase
    from opteryx.components.v2.logical_planner import do_logical_planning_phase
    from opteryx.components.v2.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    if isinstance(operation, bytes):
        operation = operation.decode()

    # SQL Rewriter removes whitespace and comments, and extracts temporal filters
    clean_sql, temporal_filters = do_sql_rewrite(operation)
    # V2: copy for v2 to process, remove this when v2 is the engine
    v2_params = [p for p in parameters or []]

    try:
        profile_content = operation + "\n\n"
        # Parser converts the SQL command into an AST
        parsed_statements = sqloxide.parse_sql(clean_sql, dialect="mysql")
        # AST Rewriter adds temporal filters and parameters to the AST
        parsed_statements = do_ast_rewriter(
            parsed_statements,
            temporal_filters=temporal_filters,
            paramters=v2_params,
            connection=connection,
        )
        # Logical Planner converts ASTs to logical plans
        for logical_plan, ast in do_logical_planning_phase(parsed_statements):
            profile_content += (
                orjson.dumps(logical_plan.depth_first_search(), option=orjson.OPT_INDENT_2).decode()
                + "\n\n"
            )
            profile_content += logical_plan.draw() + "\n\n"
            # The Binder adds schema information to the logical plan
            bound_plan = do_bind_phase(
                logical_plan,
                context=connection.context,
            )

        with open(PROFILE_LOCATION, mode="w") as f:
            f.write(profile_content)
    except Exception as err:
        raise err
