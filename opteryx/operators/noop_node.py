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
No Operation

This is a SQL Query Execution Plan Node.
"""
from typing import Generator

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class NoOpNode(BasePlanNode):

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "NoOp"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Generator:
        # nodes generally have 0 (scan), 1 (most) or 2 (join, union) producers
        if self._producers:
            for morsels in self._producers:
                yield from morsels.execute()
