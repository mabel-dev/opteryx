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
   ┌─────▼─────┐      ┌───────────┐      ╔═══════════╗
   │ AST       │      │           │Stats ║Cost-Based ║
   │ Rewriter  │      │ Catalogue ├──────► Optimizer ║
   └─────┬─────┘      └─────┬─────┘      ╚═════▲═════╝
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │ Heuristic │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘
~~~

Activities:
- order predicates for shortest time (estimated rows vs cost of execution)
- order joins for lowest effort
"""
