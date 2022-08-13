# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.
"""
from typing import Iterable

import pyarrow

from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.engine import QueryDirectives, QueryStatistics
from opteryx.engine.planner.expression import NodeType, format_expression
from opteryx.exceptions import SqlError
from opteryx.utils.columns import Columns


def replace_wildcards(arg):
    if arg.token_type == NodeType.WILDCARD:
        return "*"
    return str(arg.value)


class ProjectionNode(BasePlanNode):
    def __init__(
        self, directives: QueryDirectives, statistics: QueryStatistics, **config
    ):
        """
        Attribute Projection, remove unwanted columns and performs column renames.
        """
        super().__init__(directives=directives, statistics=statistics)
        self._projection: dict = {}

        projection = config.get("projection")
        # print("projection:", projection)
        for attribute in projection:
            if (
                attribute.token_type == NodeType.WILDCARD
                and attribute.value is not None
            ):
                # qualified wildcard, e.g. table.*
                self._projection[(attribute.value,)] = None
            elif attribute.token_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
                self._projection[format_expression(attribute)] = attribute.alias
            elif attribute.token_type == NodeType.IDENTIFIER:
                self._projection[attribute.value] = attribute.alias
            else:
                self._projection[attribute] = None

    @property
    def config(self):  # pragma: no cover
        return str(self._projection)

    @property
    def name(self):  # pragma: no cover
        return "Projection"

    def execute(self) -> Iterable:

        if len(self._producers) != 1:
            raise SqlError(f"{self.name} on expects a single producer")

        data_pages = self._producers[0]  # type:ignore
        if isinstance(data_pages, pyarrow.Table):
            data_pages = (data_pages,)

        # if we have nothing to do, move along
        if self._projection == {"*": None}:
            # print(f"projector yielding *")
            yield from data_pages.execute()
            return

        # we can't do much with this until we have a chunk to read the metadata from
        columns = None

        for page in data_pages.execute():

            # first time round we're going work out what we need from the metadata
            if columns is None:

                projection = []
                columns = Columns(page)
                for key in self._projection:
                    if isinstance(key, tuple):
                        relation = key[0]
                        projection.extend(columns.get_columns_from_source(relation))
                    else:
                        projection.append(
                            columns.get_column_from_alias(key, only_one=True)
                        )

            page = page.select(projection)  # type:ignore

            if len(projection) != len(set(projection)):
                raise SqlError(
                    "SELECT statement contains multiple references to the same column, perhaps as aliases or with qualifiers."
                )

            # then we rename the attributes
            if any([v is not None for k, v in self._projection.items()]):  # type:ignore
                existing_columns = page.column_names
                for k, v in self._projection.items():
                    if isinstance(v, list) and len(v) != 0:
                        v = v[0]
                    if v and v not in existing_columns:
                        column_name = columns.get_column_from_alias(k, only_one=True)
                        columns.set_preferred_name(column_name, v)
                page = columns.apply(page)

            yield page
