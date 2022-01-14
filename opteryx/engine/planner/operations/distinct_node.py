"""
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""
from opteryx.engine.relation import Relation
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class DistinctNode(BasePlanNode):
    def __init__(self, **config):
        self._distinct = config.get("distinct")

    def execute(self, relation: Relation) -> Relation:
        if self._distinct:
            return relation.distinct()
        return relation
