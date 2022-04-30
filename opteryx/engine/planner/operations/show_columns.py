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
Show Columns Node

This is a SQL Query Execution Plan Node.

Gives information about a dataset's columns
"""
from typing import Iterable

from pyarrow import Table

from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.attribute_types import PARQUET_TYPES
from opteryx.engine.planner.operations.base_plan_node import BasePlanNode
from opteryx.utils.columns import Columns


class ShowColumnsNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        pass

    @property
    def name(self):
        return "ShowColumns"

    @property
    def config(self):
        return ""

    def execute(self, data_pages: Iterable) -> Iterable:

        if not isinstance(data_pages, Table):
            data_pages = next(data_pages, None)  # type:ignore

        if data_pages is None:
            return None

        source_metadata = Columns(data_pages)

        buffer = []
        for column in data_pages.column_names:
            column_data = data_pages.column(column)
            new_row = {
                "column_name": source_metadata.get_preferred_name(column),
                "type": PARQUET_TYPES.get(str(column_data.type), "OTHER"),
            }
            buffer.append(new_row)

        table = Table.from_pylist(buffer)
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=len(buffer),
            name="show_columns",
            table_aliases=[],
        )
        yield table
