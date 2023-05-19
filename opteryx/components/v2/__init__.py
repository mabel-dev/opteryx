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
    import json

    from opteryx.components.v2.binder import do_bind_phase
    from opteryx.components.v2.logical_planner import do_logical_planning_phase
    from opteryx.components.v2.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    if isinstance(operation, bytes):
        operation = operation.decode()

    clean_sql, temporal_filters = do_sql_rewrite(operation)

    try:
        # V2: copy for v2 to process, remove this when v2 is the engine
        v2_params = [p for p in parameters or []]
        profile_content = operation + "\n\n"
        parsed_statements = sqloxide.parse_sql(clean_sql, dialect="mysql")
        for logical_plan, ast in do_logical_planning_phase(parsed_statements):
            profile_content += json.dumps(ast) + "\n\n"
            profile_content += logical_plan.draw() + "\n\n"
            bound_plan = do_bind_phase(
                logical_plan,
                context={},
                temporal_filters=temporal_filters,
                parameters=v2_params,
            )

        with open(PROFILE_LOCATION, mode="w") as f:
            f.write(profile_content)
    except Exception as err:
        raise err
