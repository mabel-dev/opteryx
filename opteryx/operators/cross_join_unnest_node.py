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
Cross Join Node

This is a SQL Query Execution Plan Node.

This performs a CROSS JOIN - CROSS JOIN is not natively supported by PyArrow so this is written
here rather than calling the join() functions
"""
import typing

import pyarrow
from orso.schema import FlatColumn

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode

INTERNAL_BATCH_SIZE = 100  # config
MAX_JOIN_SIZE = 500  # config


def _cross_join_unnest(
    morsels: typing.Iterable[pyarrow.Table], source: Node, target_column: FlatColumn
) -> typing.Generator[pyarrow.Table, None, None]:
    """
    Perform a cross join on an unnested column of pyarrow tables.

    Args:
        morsels: An iterable of `pyarrow.Table` objects to be cross joined.
        source: The source node indicating the column.
        target_column: The column to be unnested.

    Returns:
        A generator that yields the resulting `pyarrow.Table` objects.
    """

    # Check if the source node type is an identifier, raise error otherwise
    if source.node_type != NodeType.IDENTIFIER:
        raise NotImplementedError("Can only CROSS JOIN UNNEST on a column")

    column_type = None

    # Loop through each morsel from the morsels execution
    for left_morsel in morsels.execute():
        # Break the morsel into batches to avoid memory issues
        for left_block in left_morsel.to_batches(max_chunksize=INTERNAL_BATCH_SIZE):
            # Fetch the data of the column to be unnested
            column_data = left_block[source.schema_column.identity]

            # Set column_type if it hasn't been determined already
            if column_type is None:
                column_type = column_data.type.value_type

            # Initialize a dictionary to store new column data
            columns_data = {name: [] for name in left_block.schema.names}
            new_column_data = []

            # Loop through each value in the column data
            for i, value in enumerate(column_data):
                # Check if value is valid and non-empty
                if value.is_valid and len(value) != 0:
                    # Determine how many times a row needs to be repeated based on the length of the unnest value
                    repeat_count = len(value)

                    # Repeat the data for each column accordingly
                    for col_name in left_block.schema.names:
                        columns_data[col_name].extend(
                            [left_block.column(col_name)[i].as_py()] * repeat_count
                        )

                    # Extend the new column data with the unnested values
                    new_column_data.extend(value.as_py())
                else:
                    # If value is not valid or empty, just append the existing data
                    for col_name in left_block.schema.names:
                        columns_data[col_name].append(left_block.column(col_name)[i].as_py())
                    new_column_data.append(None)

            # If no new data was generated, skip to next iteration
            if not columns_data:
                continue

            # Convert lists to pyarrow arrays for each column
            for col_name, col_data in columns_data.items():
                columns_data[col_name] = pyarrow.array(col_data)

            # Convert new column data to pyarrow array and set its type
            columns_data[target_column.identity] = pyarrow.array(new_column_data, type=column_type)

            # Create a new table from the arrays and yield the result
            new_block = pyarrow.Table.from_arrays(
                list(columns_data.values()), names=list(columns_data.keys())
            )
            yield new_block


class CrossJoinUnnestNode(BasePlanNode):
    """
    Implements a SQL CROSS JOIN UNNEST
    """

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self.source = config.get("column")
        self.target_column = config.get("target_column")

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        if self.join_type == "cross join unnest":
            return "UNNEST()"
        return ""

    def execute(self) -> typing.Iterable:
        source_table = self._producers[0]  # type:ignore
        yield from _cross_join_unnest(
            morsels=source_table, source=self.source, target_column=self.target_column
        )
