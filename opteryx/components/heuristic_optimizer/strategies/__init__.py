from .constant_folding import ConstantFoldingStrategy
from .defragment_morsels import DefragmentMorselsStrategy
from .split_conjunctive_predicates import SplitConjunctivePredicatesStrategy

__all__ = [
    "ConstantFoldingStrategy",
    "DefragmentMorselsStrategy",
    "SplitConjunctivePredicatesStrategy",
]


# predicate rewriter (negatives, demorgans, constants)
# aggregate pushown (into SQL reader)
# subquery flattening
# CTE elimination
# rewrite LIMIT and SORT to head sort
# replace DISTINCT LIMIT with a pass-thru limiter
# replace ORDER BY LIMIT with a heap sort
# IN (<subquery>) to join
