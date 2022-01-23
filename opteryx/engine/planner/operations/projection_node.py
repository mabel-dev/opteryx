"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.
"""
from typing import Iterable
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


class ProjectionNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
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
        self._projection = config.get("projection")

    def __repr__(self):
        return str(self._projection)

    def execute(self, data_pages: Iterable) -> Iterable:

        # if we have nothing to do, move along
        if self._projection == {"*": "*"}:
            print(f"projector yielding *")
            yield from data_pages

        for page in data_pages:

            # allow simple projections using just the list of attributes
            if isinstance(self._projection, (list, tuple, set)):
                print(f"projector yielding {page.shape}")
                yield page.select(list(self._projection))

            else:
                # we elminimate attributes we don't want
                page = page.select(list(self._projection.keys()))

                # then we rename the attributes
                if any([k != v for k, v in self._projection.items()]):
                    names = [
                        self._projection[a]
                        for a in page.column_names
                        if a in self._projection
                    ]
                    page = page.rename_columns(names)

                print(f"projector yielding {page.shape}")
                yield page
