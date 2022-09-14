## the binder adds information about the data to the plan
## that would includes statistics, available columns
## it could include marking which pages are in the buffer pool, so we fetch them first


"""
SQL -> 
    Parser -> 
    AST -> (planner) -> 
    Logical Plan -> (binder) -> (heuristic optimizer) -> (cost-based optimizer) ->
    Physical Plan

"""
