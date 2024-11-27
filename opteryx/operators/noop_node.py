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

from pyarrow import Table

from opteryx.models import QueryProperties

from . import BasePlanNode


class NoOpNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "NoOp"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel: Table) -> Table:
        print("NOOP was called")
        yield morsel
