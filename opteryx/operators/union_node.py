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
Union Node

This is a SQL Query Execution Plan Node.
"""

from pyarrow import Table

from opteryx import EOS
from opteryx.models import QueryProperties

from . import BasePlanNode


class UnionNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])
        self.column_ids = [c.schema_column.identity for c in self.columns]
        self.seen_first_eos = False
        self.schema = None

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Union"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel: Table) -> Table:
        """
        Union needs to ensure the column names are the same and that
        coercible types are coerced.
        """
        if morsel == EOS and self.seen_first_eos:
            return
        elif morsel == EOS:
            self.seen_first_eos = True
            yield None

        elif self.schema is None:
            self.schema = morsel.schema
        else:
            morsel = morsel.rename_columns(self.schema.names)
            morsel = morsel.cast(self.schema)

        yield morsel.select(self.column_ids)
