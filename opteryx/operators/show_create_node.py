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
Offset Node

This is a SQL Query Execution Plan Node.

This Node skips over tuples.
"""
from typing import Iterable

import pyarrow

from opteryx.exceptions import SqlError
from opteryx.models import Columns
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode


class ShowCreateNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._table = config.get("table")

    @property
    def name(self):  # pragma: no cover
        return "ShowCreate"

    @property
    def config(self):  # pragma: no cover
        return str(self._table)

    def execute(self) -> Iterable:
        statement = f"CREATE TABLE `{self._table}` (\n"

        if len(self._producers) != 1:  # pragma: no cover
            raise SqlError(f"{self.name} on expects a single producer")

        morsels = self._producers[0]  # type:ignore
        morsel = next(morsels.execute())

        columns = Columns(morsel)
        preferred_names = columns.preferred_column_names
        column_names = []
        for col in morsel.column_names:
            column_names.append([c for a, c in preferred_names if a == col][0])
        morsel = morsel.rename_columns(column_names)

        for column in morsel.column_names:
            column_data = morsel.column(column)
            column_type = str(column_data.type)
            statement += f"\t{column.ljust(32)} {column_type},\n"
        statement += ");"

        table = pyarrow.Table.from_pylist([{"show_table_create": statement}])
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=1,
            name="show_table_create",
            table_aliases=[],
            disposition="calculated",
            path="show_create",
        )
        yield table
