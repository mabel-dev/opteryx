from .predicate_pushdown import PredicatePushdownStrategy
from .projection_pushdown import ProjectionPushdownStrategy
from .split_conjunctive_predicates import SplitConjunctivePredicatesStrategy

# predicate rewriter (negatives, demorgans, constants)
# limit pushdown
# aggregate pushown (into SQL reader)
# subquery flattening
# CTE elimination
# correlated filtering (if joining on a column with a filter - apply the filter to the other leg)
