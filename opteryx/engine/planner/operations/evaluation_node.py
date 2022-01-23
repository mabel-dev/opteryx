"""
Evaluation Node

This is a SQL Query Execution Plan Node.

"""
from typing import Iterable
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class EvaluationNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._distinct = config.get("distinct", True)

    def execute(self, data_pages: Iterable) -> Iterable:

        # for each of the items that require a calculation, do it

        yield from data_pages
