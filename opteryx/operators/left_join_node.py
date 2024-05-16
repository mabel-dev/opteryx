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
Left Join Node

This is a SQL Query Execution Plan Node.

PyArrow has a good LEFT JOIN implementation, but it errors when the
relations being joined contain STRUCT or ARRAY columns, this is true
for all of the JOIN types, however we've only written our own INNER
and LEFT JOINs.
"""
import time
from typing import Generator

import pyarrow

from opteryx.compiled.structures import HashTable
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.utils.arrow import align_tables


def left_join(left_relation, right_relation, left_columns, right_columns):
    """
    Perform an LEFT JOIN.

    This implementation ensures that all rows from the left table are included in the result set,
    with rows from the right table matched where possible, and columns from the right table
    filled with NULLs where no match is found.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        right_relation (pyarrow.Table): The right pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_columns (list of str): Column names from the right table to join on.

    Returns:
        A pyarrow.Table containing the result of the LEFT JOIN operation.
    """

    hash_table = HashTable()
    non_null_right_values = right_relation.select(right_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_right_values)):
        hash_table.insert(hash(value_tuple), i)

    left_indexes = []
    right_indexes = []

    left_values = left_relation.select(left_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*left_values)):
        rows = hash_table.get(hash(value_tuple))
        if rows:
            right_indexes.extend(rows)
            left_indexes.extend([i] * len(rows))
        else:
            right_indexes.append(None)
            left_indexes.append(i)

    # Process in chunks of 1000
    chunk_size = 1000
    for i in range(0, len(left_indexes), chunk_size):
        chunk_left_indexes = left_indexes[i : i + chunk_size]
        chunk_right_indexes = right_indexes[i : i + chunk_size]

        # Align this chunk and add the resulting table to our list
        yield align_tables(right_relation, left_relation, chunk_right_indexes, chunk_left_indexes)


class LeftJoinNode(BasePlanNode):

    operator_type = OperatorType.PASSTHRU

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)
        self._join_type = config["type"]
        self._on = config.get("on")
        self._using = config.get("using")

        self._left_columns = config.get("left_columns")
        self._left_relation = config.get("left_relation_names")

        self._right_columns = config.get("right_columns")
        self._right_relation = config.get("right_relation_names")

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return self._join_type

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        left_relation = pyarrow.concat_tables(left_node.execute(), promote_options="none")
        right_relation = pyarrow.concat_tables(right_node.execute(), promote_options="none")
        join_provider = providers.get(self._join_type)
        # in place until #1295 resolved
        if not self._left_columns[0] in left_relation.column_names:
            self._right_columns, self._left_columns = self._left_columns, self._right_columns

        start = time.monotonic_ns()
        for morsel in join_provider(
            left_relation=left_relation,
            right_relation=right_relation,
            left_columns=self._left_columns,
            right_columns=self._right_columns,
        ):
            self.statistics.time_outer_join += time.monotonic_ns() - start
            yield morsel
            start = time.monotonic_ns()


providers = {"left outer": left_join}
