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


import time
from dataclasses import dataclass
from typing import Optional

import pyarrow
from orso.tools import random_string

from opteryx import EOS


@dataclass
class BasePlanDataObject:
    operation: Optional[str] = None
    query_id: str = None
    identity: str = None

    def __post_init__(self):
        # Perform actions after initialization
        if self.identity is None:
            self.identity = random_string()
        if self.operation is None:
            self.operation = self.__class__.__name__.replace("DataObject", "Node")


class BasePlanNode:
    def __init__(self, *, properties, **parameters):
        """
        This is the base class for nodes in the execution plan.

        The initializer accepts a QueryStatistics node which is populated by different nodes
        differently to record what happened during the query execution.
        """
        from opteryx.models import QueryProperties
        from opteryx.models import QueryStatistics

        self.properties: QueryProperties = properties
        self.statistics: QueryStatistics = QueryStatistics(properties.qid)
        self.parameters = parameters
        self.execution_time = 0
        self.identity = random_string()
        self.do: Optional[BasePlanDataObject] = None
        self.calls = 0
        self.records_in = 0
        self.bytes_in = 0
        self.records_out = 0
        self.bytes_out = 0

    def to_json(self) -> bytes:  # pragma: no cover
        import orjson

        from opteryx.utils import dataclass_to_dict

        return orjson.dumps(dataclass_to_dict(self.do))

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    def config(self) -> str:
        return ""

    @property
    def name(self):  # pragma: no cover
        """
        Friendly Name of this node
        """
        return "no name"

    @property
    def node_type(self) -> str:
        return self.name

    def __str__(self) -> str:
        return f"{self.name} {self.sensors()}"

    def execute(self, morsel: pyarrow.Table) -> Optional[pyarrow.Table]:  # pragma: no cover
        pass

    def __call__(self, morsel: pyarrow.Table, join_leg: str) -> Optional[pyarrow.Table]:
        if hasattr(morsel, "num_rows"):
            self.records_in += morsel.num_rows
            self.bytes_in += morsel.nbytes
            self.calls += 1

        # set up the execution of the operator
        generator = self.execute(morsel, join_leg=join_leg)

        while True:
            try:
                # Time the production of the next result
                start_time = time.monotonic_ns()
                result = next(generator)  # Retrieve the next item from the generator
                execution_time = time.monotonic_ns() - start_time
                self.execution_time += execution_time
                self.statistics.increase("time_" + self.name.lower(), execution_time)

                # Update metrics for valid results
                if hasattr(result, "num_rows"):
                    self.records_out += result.num_rows
                    self.bytes_out += result.nbytes

                # Yield the result to the consumer
                yield result

            except StopIteration:
                # Break the loop when the generator is exhausted
                break
            except Exception as err:
                yield err

    def sensors(self):
        return {
            "calls": self.calls,
            "execution_time": self.execution_time,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        }


class JoinNode(BasePlanNode):
    pass
