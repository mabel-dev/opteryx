"""
Limit Node

This is a SQL Query Execution Plan Node.

This Node returns up to a specified number of tuples.
"""
from pyarrow import Table
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class LimitNode(BasePlanNode):
    def __init__(self, **config):
        self._limit = config.get("limit")

    def execute(self, relation: Table) -> Table:

        if self._limit is None or self._limit > relation.num_rows:
            return relation
        return relation.slice(0, self._limit)
