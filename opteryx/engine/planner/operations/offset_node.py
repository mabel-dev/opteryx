"""
Offset Node

This is a SQL Query Execution Plan Node.

This Node skips over tuples.
"""
from typing import Iterable
from pyarrow import concat_tables
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class OffsetNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        self._offset = config.get("offset")

    def execute(self, data_pages: Iterable) -> Iterable:

        row_count = 0

        for page in data_pages:
            if (row_count + page.num_rows) > self._offset:
                page = page.slice(self._offset - row_count, page.num_rows)
                yield page
                break
            else:
                row_count += page.num_rows

        yield from data_pages
