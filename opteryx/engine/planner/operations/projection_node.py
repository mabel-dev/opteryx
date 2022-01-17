"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.
"""
from typing import Optional
from pyarrow import Table
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


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
        self._projection = kwargs.get("projection")

    def __repr__(self):
        return str(self._projection)

    def execute(self, relation: Table) -> Optional[Table]:

        # allow simple projections using just the list of attributes
        if isinstance(self._projection, (list, tuple, set)):
            return relation.select(list(self._projection))

        # if we have nothing to do, move along
        if self._projection == {"*": "*"} or relation == None:
            return relation

        # we elminimate attributes we don't want
        relation = relation.select(list(self._projection.keys()))

        # then we rename the attributes
        if any([k != v for k, v in self._projection.items()]):
            names = [
                self._projection[a]
                for a in relation.column_names
                if a in self._projection
            ]
            relation = relation.rename_columns(names)

        return relation
