"""
Limit Node

This is a SQL Query Execution Plan Node.

This Node returns up to a specified number of tuples.
"""
from typing import Optional
from opteryx.engine.relation import Relation
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class LimitNode(BasePlanNode):
    def __init__(self, config):
        self._limit = config

    def execute(self, relation: Relation) -> Relation:

        if self._limit is None:
            return relation

        # limit the number of records by slicing the underlying data array
        relation.materialize()

        relation.data = relation.data[0 : self._limit]
        return relation
