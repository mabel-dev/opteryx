"""
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""
from typing import Optional
from opteryx.engine.relation import Relation
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class DistinctNode(BasePlanNode):

    def __init__(self, **kwargs):
        pass

    def execute(self, relation:Relation) -> Optional[Relation]:
        return relation.distinct()