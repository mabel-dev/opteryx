"""
This is a naive initial impementation of a query optimizer.

It is simply a set of rules which are executed in turn.

Query optimizers are the magic in a query engine, this is not magic, but all complex
things emerged from simple things, we we've set a low bar to get started on
implementing optimization.

The initial optimizer will be heuristic (rule-based)
"""
import time

from opteryx.managers.planner.optimizer import actions
from opteryx.shared.query_statistics import QueryStatistics

RULESET: list = [
    actions.apply_demorgans_law,
    actions.eliminate_negations,
    actions.split_conjunctive_predicates,  # run after eliminate_negations
    actions.eliminate_fixed_function_evaluations,  # run before constant evaluations
    actions.eliminate_constant_evaluations,
    actions.defragment_pages,
    actions.use_heap_sort,
]


def run_optimizer(plan, properties):

    stats = QueryStatistics(properties.qid)
    start = time.monotonic_ns()

    for rule in RULESET:
        plan = rule(plan, properties)

    stats.time_optimizing += time.monotonic_ns() - start

    return plan
