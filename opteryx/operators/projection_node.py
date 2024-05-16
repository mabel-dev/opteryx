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
from typing import Generator

from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class ProjectionNode(BasePlanNode):

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        """
        Attribute Projection, remove unwanted columns and performs column renames.
        """
        super().__init__(properties=properties)

        projection = config["projection"] + config.get("order_by_columns", [])

        self.projection = []
        for column in projection:
            self.projection.append(column.schema_column.identity)

        self.evaluations = [
            column for column in projection if column.node_type != NodeType.IDENTIFIER
        ]

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return str(self.projection)

    @property
    def name(self):  # pragma: no cover
        return "Projection"

    def execute(self) -> Generator:
        morsels = self._producers[0]  # type:ignore

        for morsel in morsels.execute():
            # If any of the columns need evaluating, we need to do that here
            start_time = time.time_ns()
            morsel = evaluate_and_append(self.evaluations, morsel)
            self.statistics.time_evaluating += time.time_ns() - start_time

            morsel = morsel.select(self.projection)

            yield morsel
