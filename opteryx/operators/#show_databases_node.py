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
Show Stores Node

This is a SQL Query Execution Plan Node.
"""
from typing import Iterable

import pyarrow

from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


class ShowDatabasesNode(BasePlanNode):

    operator_type = OperatorType.PRODUCER

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Show Databases"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:
        from opteryx.connectors import _storage_prefixes

        buffer = [
            {
                "Database": "<default>" if s == "_" else s,  # type: ignore
                "Connector": str(c["connector"].__name__),  # type: ignore
                "Remove_Prefix": c["remove_prefix"],  # type: ignore
                "Type": str(c["connector"].mro()[1].__name__[4:-14]),  # type: ignore
            }
            for s, c in _storage_prefixes.items()
            if isinstance(c, dict)
        ]
        buffer.append(
            {
                "Database": "opteryx",  # type: ignore
                "Connector": "Internal",  # type: ignore
                "Remove_Prefix": True,  # type: ignore
                "Type": "Internal",  # type: ignore
            }
        )

        table = pyarrow.Table.from_pylist(buffer)
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=len(buffer),
            name="show_stores",
            table_aliases=[],
            disposition="calculated",
            path="show_stores",
        )

        yield table
        return
