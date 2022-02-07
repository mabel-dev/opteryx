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
        self._projection = []

        projection = config.get("projection", {"*": "*"})
        # print("projection:", projection)
        for attribute in projection:
            if "aggregate" in attribute:
                self._projection.append(
                    f"{attribute['aggregate']}({','.join([replace_wildcards(a) for a in attribute['args']])})"
                )
            elif "function" in attribute:
                self._projection.append(
                    f"{attribute['function']}({','.join([replace_wildcards(a) for a in attribute['args']])})"
                )
            else:
                self._projection.append(attribute)

    def __repr__(self):
        return str(self._projection)

    def execute(self, data_pages: Iterable) -> Iterable:

        from opteryx.third_party.pyarrow_ops.group import Grouping

        # if we have nothing to do, move along
        if self._projection == ["*"]:
            # print(f"projector yielding *")
            yield from data_pages

        for page in data_pages:

            # allow simple projections using just the list of attributes
            if isinstance(self._projection, (list, tuple, set)):
                # print(f"projector yielding {page.shape}")
                try:
                    yield page.select(list(self._projection))
                except KeyError as e:
                    # print(f"Available columns: {page.column_names}")
                    raise e

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

                # print(f"projector yielding {page.shape}")
                yield page
