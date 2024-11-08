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
Explain Node

This is a SQL Query Execution Plan Node.

This writes out a query plan
"""

from pyarrow import Table

from opteryx.models import QueryProperties

from . import BasePlanNode


class ExplainNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._query_plan = parameters.get("query_plan")
        self.analyze = parameters.get("analyze", False)

    @property
    def name(self):  # pragma: no cover
        return "Explain"

    @property  # pragma: no cover
    def config(self):
        return ""

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    def execute(self, morsel: Table) -> Table:
        if self._query_plan:
            return self._query_plan.explain(self.analyze)
