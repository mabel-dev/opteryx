"""
This is a initial impementation of a query optimizer.

It is implemented as a set of rules which are executed in turn. It is a heuristic,
no regrets optimizer and rewriter.
"""
import time

from opteryx.components.tree_rewriter import rules
from opteryx.shared.query_statistics import QueryStatistics

RULESET: list = [
    # left joins
    rules.move_literal_join_filters,
    rules.apply_demorgans_law,
    rules.eliminate_negations,
    rules.split_conjunctive_predicates,  # run after eliminate_negations
    rules.eliminate_fixed_function_evaluations,  # run before constant evaluations
    rules.eliminate_constant_evaluations,
    rules.predicate_pushdown,
    rules.defragment_morsels,
    rules.use_heap_sort,
]


def tree_rewriter(plan, properties):
    stats = QueryStatistics(properties.qid)
    start = time.monotonic_ns()

    for rule in RULESET:
        plan = rule(plan, properties)

    stats.time_rewriting += time.monotonic_ns() - start

    return plan
