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
Show Variables Node

This is a SQL Query Execution Plan Node.
"""
from typing import Generator

import pyarrow

from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class ShowValueNode(BasePlanNode):

    operator_type = OperatorType.PRODUCER

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

        self.key = config.get("key")
        self.kind = config.get("kind")
        self.value = config.get("value")

        if self.kind == "PARAMETER":
            if self.value[0] == "@":
                raise SqlError("PARAMETERS cannot start with '@'")
            self.key = self.value
            self.value = properties.variables[self.value]

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Show Value"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Generator:
        buffer = [{"name": self.key, "value": str(self.value)}]
        table = pyarrow.Table.from_pylist(buffer)
        yield table
