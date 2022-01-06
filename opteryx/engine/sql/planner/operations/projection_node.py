"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.
"""
from typing import Optional
from opteryx.engine.relation import Relation
from opteryx.engine.sql.planner.operations.base_plan_node import BasePlanNode


class ProjectionNode(BasePlanNode):

    def __init__(self, **kwargs):
        """
        Attribute Projection, remove unwanted columns and performs column renames.

        Paramters:
            projection: dict
                dictionary in the following form:
                {
                    source_attribute 1 : projected_attribute 1
                    source_attribute 2 : projected_attribute 2
                }
        """
        self._projection = kwargs.get('projection', {"*":"*"})

    def execute(self, relation:Relation) -> Optional[Relation]:

        # if we have nothing to do, move along
        if self._projection == {"*":"*"} or relation == None:
            return relation

        # first we rename the attributes - we do this by manipulating the header
        for source, target in self._projection.items():
            relation.rename_attribute(source, target)

        # then we order elminimate and order the resultant attributes
        return relation.apply_projection(list(self._projection.values()))
