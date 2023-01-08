"""
This is the SQL Rewriter phase

It is disctinct from the optimizer because the rewriter isn't trying to improve
performance and therefore cannot be disabled.
"""
import time

from opteryx.managers.planner.rewriter import actions
from opteryx.shared.query_statistics import QueryStatistics

RULESET: list = [actions.move_literal_join_filters]


def run_rewriter(plan, properties):

    stats = QueryStatistics(properties.qid)
    start = time.monotonic_ns()

    for rule in RULESET:
        plan = rule(plan, properties)

    stats.time_rewriting += time.monotonic_ns() - start

    return plan
