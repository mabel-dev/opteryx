# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Outer Join Node

This is a SQL Query Execution Plan Node.

PyArrow has LEFT/RIGHT/FULL OUTER JOIN implementations, but they error when the
relations being joined contain STRUCT or ARRAY columns so we've written our own
OUTER JOIN implementations.

We also have our own INNER JOIN implementations, it's really just the less
popular SEMI and ANTI joins we leave to PyArrow for now.
"""

from typing import List

import pyarrow

from opteryx import EOS
from opteryx.compiled.structures import HashTable
from opteryx.compiled.structures.buffers import IntBuffer
from opteryx.models import QueryProperties
from opteryx.third_party.abseil.containers import FlatHashMap
from opteryx.utils.arrow import align_tables

from . import JoinNode


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
    from opteryx.compiled.joins.inner_join import abs_hash_join_map
    from opteryx.compiled.structures.hash_table import hash_join_map

    left_indexes = IntBuffer()
    right_indexes = []

    if len(set(left_columns) & set(right_relation.column_names)) > 0:
        left_columns, right_columns = right_columns, left_columns

    right_hash = abs_hash_join_map(right_relation, right_columns)
    left_hash = hash_join_map(left_relation, left_columns)

    for hash_value, left_rows in left_hash.hash_table.items():
        right_rows = right_hash.get(hash_value)
        if right_rows:
            for l in left_rows:
                left_indexes.extend([l] * len(right_rows))
                right_indexes.extend(right_rows)
        else:
            for l in left_rows:
                left_indexes.append(l)
                right_indexes.append(None)

        if left_indexes.size > 50_000:
            table = align_tables(
                right_relation, left_relation, right_indexes, left_indexes.to_numpy()
            )
            yield table
            left_indexes.size = 0
            right_indexes.clear()

    # this may return an empty table each time - fix later
    table = align_tables(right_relation, left_relation, right_indexes, left_indexes.to_numpy())
    yield table


def full_join(left_relation, right_relation, left_columns: List[str], right_columns: List[str]):
    chunk_size = 1000

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

    hash_table = FlatHashMap()
    non_null_left_values = left_relation.select(left_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_left_values)):
        hash_table.insert(hash(value_tuple), i)

    # Iterate over the right_relation in chunks

    for right_chunk in right_relation.to_batches(chunk_size):
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


class OuterJoinNode(JoinNode):
    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)
        self.join_type = parameters["type"]
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

        self.left_buffer = []
        self.right_buffer = []
        self.left_relation = None

    @property
    def name(self):  # pragma: no cover
        return self.join_type

    @property
    def config(self) -> str:  # pragma: no cover
        from opteryx.managers.expression import format_expression

        if self.on:
            return f"{self.join_type.upper()} JOIN ({format_expression(self.on, True)})"
        if self.using:
            return f"{self.join_type.upper()} JOIN (USING {','.join(map(format_expression, self.using))})"
        return f"{self.join_type.upper()}"

    def execute(self, morsel: pyarrow.Table, join_leg: str) -> pyarrow.Table:
        if join_leg == "left":
            if morsel == EOS:
                self.left_relation = pyarrow.concat_tables(self.left_buffer, promote_options="none")
                self.left_buffer.clear()
            else:
                self.left_buffer.append(morsel)
            yield None
            return

        if join_leg == "right":
            if morsel == EOS:
                right_relation = pyarrow.concat_tables(self.right_buffer, promote_options="none")
                self.right_buffer.clear()

                join_provider = providers.get(self.join_type)

                yield from join_provider(
                    left_relation=self.left_relation,
                    right_relation=right_relation,
                    left_columns=self.left_columns,
                    right_columns=self.right_columns,
                )
                yield EOS

            else:
                self.right_buffer.append(morsel)
                yield None


providers = {"left outer": left_join, "full outer": full_join, "right outer": right_join}
