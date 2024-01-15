from .constant_folding import ConstantFoldingStrategy
from .defragment_morsels import DefragmentMorselsStrategy
from .in_subquery_to_join import InSubQueryToJoinStrategy
from .split_conjunctive_predicates import SplitConjunctivePredicatesStrategy

__all__ = [
    "ConstantFoldingStrategy",
    "DefragmentMorselsStrategy",
    "InSubQueryToJoinStrategy",
    "SplitConjunctivePredicatesStrategy",
]


# predicate rewriter (negatives, demorgans, constants)
# aggregate pushown (into SQL reader)
# subquery flattening
# CTE elimination
# rewrite LIMIT and SORT to head sort
# replace DISTINCT LIMIT with a pass-thru limiter
# replace ORDER BY LIMIT with a heap sort
