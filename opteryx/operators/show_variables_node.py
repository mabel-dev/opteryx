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
from typing import Iterable
import pyarrow
from opteryx.exceptions import SqlError

from opteryx.models import Columns
from opteryx.operators import BasePlanNode


class ShowVariablesNode(BasePlanNode):
    @property
    def name(self):  # pragma: no cover
        return "Show Variables"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Iterable:

        buffer = []

        if len(self.properties.variables) == 0:
            raise SqlError(
                "Variables must be created using `SET` statements before `SHOW VARIABLES`."
            )

        for variable, value in self.properties.variables.items():
            buffer.append({"name": variable, "value": str(value.value)})

        table = pyarrow.Table.from_pylist(buffer)
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=len(buffer),
            name="show_variables",
            table_aliases=[],
        )

        yield table
        return
