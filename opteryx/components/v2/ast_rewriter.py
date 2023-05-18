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
   ╔═════▼═════╗      ┌───────────┐      ┌─────┴─────┐
   ║ AST       ║      │           │Stats │           │
   ║ Rewriter  ║      │ Catalogue ├──────► Optimizer │
   ╚═════╦═════╝      └─────┬─────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Tree      │
   │   Planner ├──────► Binder    ├──────►  Rewriter │
   └───────────┘      └───────────┘      └───────────┘
~~~
"""


def temporal_range_binder(ast, filters):
    if isinstance(ast, (list)):
        return [temporal_range_binder(node, filters) for node in ast]
    if isinstance(ast, (dict)):
        node_name = next(iter(ast))
        if node_name == "Table":
            temporal_range = filters.pop(0)
            ast["Table"]["start_date"] = temporal_range[1]
            ast["Table"]["end_date"] = temporal_range[2]
            return ast
        if "table_name" in ast:
            temporal_range = filters.pop(0)
            ast["table_name"][0]["start_date"] = temporal_range[1]
            ast["table_name"][0]["end_date"] = temporal_range[2]
            return ast
        if "ShowCreate" in ast:
            temporal_range = filters.pop(0)
            ast["ShowCreate"]["start_date"] = temporal_range[1]
            ast["ShowCreate"]["end_date"] = temporal_range[2]
            return ast
        return {k: temporal_range_binder(v, filters) for k, v in ast.items()}
    return ast
