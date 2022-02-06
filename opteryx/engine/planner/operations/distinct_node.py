"""
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""
from typing import Iterable
from pyarrow import concat_tables
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.third_party.pyarrow_ops import drop_duplicates


class DistinctNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._distinct = config.get("distinct", True)

    def __repr__(self) -> str:
        return ""

    def greedy(self):
        return True

    def execute(self, data_pages: Iterable) -> Iterable:
        if self._distinct:
            yield drop_duplicates(concat_tables(data_pages))
        yield from data_pages
