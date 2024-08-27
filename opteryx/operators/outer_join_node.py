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
Outer Join Node

This is a SQL Query Execution Plan Node.

PyArrow has LEFT/RIGHT/FULL OUTER JOIN implementations, but they error when the
relations being joined contain STRUCT or ARRAY columns so we've written our own
OUTER JOIN implementations.

We also have our own INNER JOIN implementations, it's really just the less
popular SEMI and ANTI joins we leave to PyArrow for now.
"""

import time
from typing import Generator
from typing import List

import pyarrow

from opteryx.compiled.structures import HashTable
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.utils.arrow import align_tables


def left_join(left_relation, right_relation, left_columns: List[str], right_columns: List[str]):
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
    from collections import deque

    from opteryx.compiled.structures.hash_table import hash_join_map

    left_indexes = deque()
    right_indexes = deque()

    right_relation = pyarrow.concat_tables(right_relation.execute(), promote_options="none")
    right_hash = hash_join_map(right_relation, right_columns)

    for left_batch in left_relation.execute():
        left_hash = hash_join_map(left_batch, left_columns)
        for hash_value, left_rows in left_hash.hash_table.items():
            right_rows = right_hash.get(hash_value)
            if right_rows:
                for l in left_rows:
                    for r in right_rows:
                        left_indexes.append(l)
                        right_indexes.append(r)
            else:
                for l in left_rows:
                    left_indexes.append(l)
                    right_indexes.append(None)

            if len(left_indexes) > 50_000:
                table = align_tables(
                    right_relation, left_batch, list(right_indexes), list(left_indexes)
                )
                yield table
                left_indexes.clear()
                right_indexes.clear()

        if len(left_indexes) > 0:
            table = align_tables(
                right_relation, left_batch, list(right_indexes), list(left_indexes)
            )
            yield table
            left_indexes.clear()
            right_indexes.clear()


def full_join(left_relation, right_relation, left_columns: List[str], right_columns: List[str]):
    chunk_size = 1000
    right_relation = pyarrow.concat_tables(right_relation.execute(), promote_options="none")

    hash_table = HashTable()
    non_null_right_values = right_relation.select(right_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_right_values)):
        hash_table.insert(hash(value_tuple), i)

    left_indexes = []
    right_indexes = []

    left_relation = pyarrow.concat_tables(left_relation.execute(), promote_options="none")
    left_values = left_relation.select(left_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*left_values)):
        rows = hash_table.get(hash(value_tuple))
        if rows:
            right_indexes.extend(rows)
            left_indexes.extend([i] * len(rows))
        else:
            right_indexes.append(None)
            left_indexes.append(i)

    for i in range(right_relation.num_rows):
        if i not in right_indexes:
            right_indexes.append(i)
            left_indexes.append(None)

    for i in range(0, len(left_indexes), chunk_size):
        chunk_left_indexes = left_indexes[i : i + chunk_size]
        chunk_right_indexes = right_indexes[i : i + chunk_size]

        # Align this chunk and add the resulting table to our list
        yield align_tables(right_relation, left_relation, chunk_right_indexes, chunk_left_indexes)


def right_join(left_relation, right_relation, left_columns: List[str], right_columns: List[str]):
    """
    Perform a RIGHT JOIN.

    This implementation ensures that all rows from the right table are included in the result set,
    with rows from the left table matched where possible, and columns from the left table
    filled with NULLs where no match is found.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        right_relation (pyarrow.Table): The right pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_columns (list of str): Column names from the right table to join on.

    Yields:
        pyarrow.Table: A chunk of the result of the RIGHT JOIN operation.
    """
    chunk_size = 1000
    left_relation = pyarrow.concat_tables(left_relation.execute(), promote_options="none")

    hash_table = HashTable()
    non_null_left_values = left_relation.select(left_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_left_values)):
        hash_table.insert(hash(value_tuple), i)

    # Iterate over the right_relation in chunks
    right_batches = right_relation.execute()
    for right_batch in right_batches:
        for right_chunk in right_batch.to_batches(chunk_size):
            left_indexes = []
            right_indexes = []

            right_values = right_chunk.select(right_columns).itercolumns()
            for i, value_tuple in enumerate(zip(*right_values)):
                rows = hash_table.get(hash(value_tuple))
                if rows:
                    left_indexes.extend(rows)
                    right_indexes.extend([i] * len(rows))
                else:
                    left_indexes.append(None)
                    right_indexes.append(i)

            # Yield the aligned chunk
            # we intentionally swap them to the other calls so we're building a table
            # not a record batch (what the chunk is)
            yield align_tables(left_relation, right_chunk, left_indexes, right_indexes)


def left_anti_join(
    left_relation, right_relation, left_columns: List[str], right_columns: List[str]
):
    """
    Perform a LEFT ANTI JOIN.

    This implementation ensures that all rows from the left table are included in the result set,
    where there are no matching rows in the right table based on the join columns.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        right_relation (pyarrow.Table): The right pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_columns (list of str): Column names from the right table to join on.

    Returns:
        A pyarrow.Table containing the result of the LEFT ANTI JOIN operation.
    """
    right_relation = pyarrow.concat_tables(right_relation.execute(), promote_options="none")

    hash_table = HashTable()
    non_null_right_values = right_relation.select(right_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_right_values)):
        hash_table.insert(hash(value_tuple), i)

    at_least_once = False
    # Iterate over the left_relation in chunks
    for left_batch in left_relation.execute():
        left_indexes = []
        left_values = left_batch.select(left_columns).itercolumns()
        for i, value_tuple in enumerate(zip(*left_values)):
            rows = hash_table.get(hash(value_tuple))
            if not rows:  # Only include left rows that have no match in the right table
                left_indexes.append(i)

        # Filter the left_chunk based on the anti join condition
        if left_indexes:
            yield left_batch.take(left_indexes)
            at_least_once = True

    if not at_least_once:
        yield left_batch.slice(0, 0)


def left_semi_join(
    left_relation, right_relation, left_columns: List[str], right_columns: List[str]
):
    """
    Perform a LEFT SEMI JOIN.

    This implementation ensures that all rows from the left table that have a matching row in the right table
    based on the join columns are included in the result set.

    Parameters:
        left_relation (pyarrow.Table): The left pyarrow.Table to join.
        right_relation (pyarrow.Table): The right pyarrow.Table to join.
        left_columns (list of str): Column names from the left table to join on.
        right_columns (list of str): Column names from the right table to join on.

    Returns:
        A pyarrow.Table containing the result of the LEFT SEMI JOIN operation.
    """
    right_relation = pyarrow.concat_tables(right_relation.execute(), promote_options="none")

    hash_table = HashTable()
    non_null_right_values = right_relation.select(right_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_right_values)):
        hash_table.insert(hash(value_tuple), i)

    at_least_once = False
    # Iterate over the left_relation in chunks
    for left_batch in left_relation.execute():
        left_indexes = []
        left_values = left_batch.select(left_columns).itercolumns()

        for i, value_tuple in enumerate(zip(*left_values)):
            rows = hash_table.get(hash(value_tuple))
            if rows:  # Only include left rows that have a match in the right table
                left_indexes.append(i)

        # Filter the left_chunk based on the anti join condition
        if left_indexes:
            yield left_batch.take(left_indexes)
            at_least_once = True

    if not at_least_once:
        yield left_batch.slice(0, 0)


class OuterJoinNode(BasePlanNode):
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
        from opteryx.managers.expression import format_expression

        if self._on:
            return f"{self._join_type.upper()} JOIN ({format_expression(self._on, True)})"
        if self._using:
            return f"{self._join_type.upper()} JOIN (USING {','.join(map(format_expression, self._using))})"
        return f"{self._join_type.upper()}"

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        join_provider = providers.get(self._join_type)

        start = time.monotonic_ns()
        for morsel in join_provider(
            left_relation=left_node,
            right_relation=right_node,
            left_columns=self._left_columns,
            right_columns=self._right_columns,
        ):
            self.statistics.time_outer_join += time.monotonic_ns() - start
            yield morsel
            start = time.monotonic_ns()


providers = {
    "left outer": left_join,
    "full outer": full_join,
    "right outer": right_join,
    "left anti": left_anti_join,
    "left semi": left_semi_join,
}
