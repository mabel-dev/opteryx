"""
Limit Node

This is a SQL Query Execution Plan Node.

This Node returns up to a specified number of tuples.
"""
from typing import Optional
from opteryx.engine.relation import Relation
from opteryx.engine.sql.planner.operations.base_plan_node import BasePlanNode


class LimitNode(BasePlanNode):

    def __init__(self, **kwargs):
        self._limit = kwargs.get('limit', -1)

    def execute(self, relation:Relation) -> Optional[Relation]:

        # limit the number of records by slicing the underlying data array
        if not isinstance(relation.data, list):
            relation.data = list(relation.data)

        relation.data = relation.data[0:self._limit]
        return relation