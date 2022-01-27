"""
Grouping Node

This is a SQL Query Execution Plan Node.

This performs the grouping, but not the aggregations.
"""
from typing import Iterable
from opteryx.engine import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.third_party.pyarrow_ops import groupby


class GroupNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._groups = config.get("groups", [])

    def execute(self, data_pages: Iterable) -> Iterable:

        for page in data_pages:
            yield groupby(page, self._groups)
