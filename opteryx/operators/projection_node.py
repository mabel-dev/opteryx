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
import time
from typing import Iterable

from orso.tools import random_int

from opteryx.exceptions import SqlError
from opteryx.managers.expression import LITERAL_TYPE
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import format_expression
from opteryx.models import QueryProperties
from opteryx.models.node import Node
from opteryx.operators import BasePlanNode


class ProjectionNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        """
        Attribute Projection, remove unwanted columns and performs column renames.
        """
        super().__init__(properties=properties)
        self._projection: dict = {}
        self._expressions = []

        projection = config.get("projection", [])
        for attribute in projection:
            while attribute.node_type == NodeType.NESTED:
                attribute = attribute.centre
            if attribute.node_type == NodeType.WILDCARD and attribute.value is not None:
                # qualified wildcard, e.g. table.*
                self._projection[attribute.value] = None
            elif attribute.node_type in (
                NodeType.FUNCTION,
                NodeType.AGGREGATOR,
                NodeType.COMPLEX_AGGREGATOR,
                NodeType.BINARY_OPERATOR,
                NodeType.COMPARISON_OPERATOR,
            ) or (attribute.node_type & LITERAL_TYPE == LITERAL_TYPE):
                new_column_name = format_expression(attribute)
                self._projection[new_column_name] = attribute.alias
                self._expressions.append(attribute)
            elif attribute.node_type == NodeType.IDENTIFIER:
                self._projection[attribute.value] = attribute.alias
            elif isinstance(attribute, Node):
                new_column_name = format_expression(attribute)
                self._projection[new_column_name] = attribute.alias
                self._expressions.append(attribute)
            else:
                self._projection[attribute] = None

    @property
    def config(self):  # pragma: no cover
        return str(self._projection)

    @property
    def name(self):  # pragma: no cover
        return "Projection"

    def execute(self) -> Iterable:
        if len(self._producers) != 1:  # pragma: no cover
            raise SqlError(f"{self.name} expects a single producer")

        morsels = self._producers[0]  # type:ignore

        # if we have nothing to do, move along
        if self._projection == {"*": None}:
            yield from morsels.execute()
            return

        # we can't do much with this until we have a chunk to read the metadata from
        columns = None
        # we want to avoid collisions in internal column names
        seed = str(random_int() + 1)

        for morsel in morsels.execute():
            # If any of the columns are FUNCTIONs, we need to evaluate them
            start_time = time.time_ns()
            _columns, _, morsel = evaluate_and_append(self._expressions, morsel, seed)
            self.statistics.time_evaluating += time.time_ns() - start_time

            projection = []
            for column in self._projection:
                projection.append(column)

            morsel = morsel.select(projection)  # type:ignore

            if len(projection) != len(set(projection)):
                raise SqlError(
                    "SELECT statement contains multiple references to the same column, perhaps as aliases or with qualifiers."
                )

            # then we rename the attributes
            if any([v is not None for k, v in self._projection.items()]):  # type:ignore
                existing_columns = morsel.column_names
                for k, v in self._projection.items():
                    if isinstance(v, list) and len(v) != 0:
                        v = v[0]
                    if v and v not in existing_columns:
                        column_name = columns.get_column_from_alias(k, only_one=True)
                        columns.set_preferred_name(column_name, v)
                morsel = columns.apply(morsel)

            yield morsel
