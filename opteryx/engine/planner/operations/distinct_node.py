"""
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""
from opteryx.engine.relation import Relation
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.third_party.pyarrow_ops import drop_duplicates


class DistinctNode(BasePlanNode):
    def __init__(self, **config):
        self._distinct = config.get("distinct", True)

    def execute(self, relation: Relation) -> Relation:
        if self._distinct:
            return drop_duplicates(relation)
        return relation
