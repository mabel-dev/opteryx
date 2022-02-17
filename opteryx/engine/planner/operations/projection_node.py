"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.
"""
from typing import Iterable
from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode


def replace_wildcards(arg):
    if arg[1] == TOKEN_TYPES.WILDCARD:
        return "*"
    return str(arg[0])


class ProjectionNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        """
        Attribute Projection, remove unwanted columns and performs column renames.
        """
        self._projection = {}

        projection = config.get("projection", {"*": "*"})
        # print("projection:", projection)
        for attribute in projection:
            if "aggregate" in attribute:
                self._projection[
                    f"{attribute['aggregate']}({','.join([replace_wildcards(a) for a in attribute['args']])})"
                ] = attribute["alias"]

            elif "function" in attribute:
                self._projection[
                    f"{attribute['function']}({','.join([replace_wildcards(a) for a in attribute['args']])})"
                ] = attribute["alias"]

            elif "identifier" in attribute:
                self._projection[attribute["identifier"]] = attribute["alias"]
            else:
                self._projection[attribute] = None

    def __repr__(self):
        return str(self._projection)

    def execute(self, data_pages: Iterable) -> Iterable:

        # if we have nothing to do, move along
        if self._projection == {"*": None}:
            # print(f"projector yielding *")
            yield from data_pages
            return

        for page in data_pages:

            # we elminimate attributes we don't want
            page = page.select(self._projection.keys())  # type:ignore

            # then we rename the attributes
            if any([v is not None for k, v in self._projection.items()]):  # type:ignore
                names = [
                    self._projection[a] or a  # type:ignore
                    for a in page.column_names
                    if a in self._projection  # type:ignore
                ]
                page = page.rename_columns(names)

            yield page
