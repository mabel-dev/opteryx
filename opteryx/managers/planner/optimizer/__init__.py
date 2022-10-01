"""
This is a naive initial impementation of a query optimizer.

It is simply a set of rules which are executed in turn.

Query optimizers are the magic in a query engine, this is not magic, but all complex
things emerged from simple things, we we've set a low bar to get started on
implementing optimization.

The initial optimizer will be heuristic (rule-based)
"""

from opteryx.managers.planner.optimizer import actions

RULESET: list = [actions.split_conjunctive_predicates]


def run_optimizer(plan):

    for rule in RULESET:
        plan = rule(plan)

    return plan
