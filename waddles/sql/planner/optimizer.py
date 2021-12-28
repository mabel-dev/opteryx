"""
Query Optimizer
---------------

This is a heuristic-based query optimizer:
    → Perform most restrictive selection early
    → Perform all selections before joins
    → Predicate/Limit/Projection pushdowns
    → Join ordering based on cardinality
"""
