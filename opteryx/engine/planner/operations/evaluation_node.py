"""
Evaluation Node

This is a SQL Query Execution Plan Node.

This performs aliases and resolves function calls.
"""
from typing import Iterable
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class EvaluationNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self.projection = config.get("projection")

    def execute(self, data_pages: Iterable) -> Iterable:

        for page in data_pages:

            # for function, add calculate and add column

            # for alias, add aliased column

            yield page
