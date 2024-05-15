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


from enum import Enum
from enum import auto
from typing import Generator

import pyarrow
from orso.tools import random_string

from opteryx.models import QueryProperties
from opteryx.models import QueryStatistics


class OperatorType(int, Enum):
    PRODUCER = auto()
    PASSTHRU = auto()
    BLOCKING = auto()
    _UNKNOWN = auto()


class BasePlanNode:

    _producers = None
    operator_type = OperatorType._UNKNOWN

    def __init__(self, properties: QueryProperties, **parameters):
        """
        This is the base class for nodes in the execution plan.

        The initializer accepts a QueryStatistics node which is populated by different nodes
        differently to record what happened during the query execution.
        """
        self.properties = properties
        self.parameters = parameters
        self.statistics = QueryStatistics(properties.qid)
        self.execution_time = 0
        self.identity = random_string()

    def to_json(self) -> dict:  # pragma: no cover
        raise NotImplementedError()

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    def set_producers(self, producers):
        self._producers = producers

    def config(self) -> str:
        return ""

    @property
    def name(self):  # pragma: no cover
        """
        Friendly Name of this node
        """
        return "no name"

    def execute(self) -> Generator[pyarrow.Table, None, None]:  # pragma: no cover
        pass
