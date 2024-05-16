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
from typing import Generator

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class UnionNode(BasePlanNode):

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.columns = config.get("columns", [])
        self.column_ids = [c.schema_column.identity for c in self.columns]

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Union"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Generator:
        """
        Union needs to ensure the column names are the same and that
        coercible types are coerced.
        """
        schema = None
        if self._producers:
            for morsels in self._producers:
                for morsel in morsels.execute():
                    if schema is None:
                        schema = morsel.schema
                    else:
                        morsel = morsel.rename_columns(schema.names)
                        morsel = morsel.cast(schema)
                    yield morsel.select(self.column_ids)
