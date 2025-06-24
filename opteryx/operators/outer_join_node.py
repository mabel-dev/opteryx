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
"""

import time
from typing import List

import pyarrow

from opteryx import EOS
from opteryx.compiled.joins.inner_join import build_side_hash_map
from opteryx.compiled.joins.outer_join import probe_side_hash_map
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.compiled.structures.buffers import IntBuffer
from opteryx.compiled.structures.hash_table import HashTable
from opteryx.models import QueryProperties
from opteryx.third_party.abseil.containers import FlatHashMap
from opteryx.utils.arrow import align_tables

from . import JoinNode

CHUNK_SIZE: int = 50_000


def left_join(
    left_relation,
    right_relation,
    left_columns: List[str],
    right_columns: List[str],
    filter_index,
    left_hash,
):
    """
    Perform a LEFT OUTER JOIN using a prebuilt hash map and optional filter.

    Yields:
        pyarrow.Table chunks of the joined result.
    """

    left_indexes = IntBuffer()
    right_indexes = IntBuffer()
    seen_left_rows = set()

    if filter_index:
        # We can just dispose of rows from the right relation that don't match
        # our bloom filter
        possibly_matching_rows = filter_index.possibly_contains_many(right_relation, right_columns)
        right_relation = right_relation.filter(possibly_matching_rows)

        # If there's no matching rows in the right relation, we can exit early
        if right_relation.num_rows == 0:
            # Short circuit: no matching right rows at all
            for i in range(0, left_relation.num_rows, CHUNK_SIZE):
                chunk = list(range(i, min(i + CHUNK_SIZE, left_relation.num_rows)))
                yield align_tables(
                    source_table=left_relation,
                    append_table=right_relation.slice(0, 0),
                    source_indices=chunk,
                    append_indices=[None] * len(chunk),
                )
            return

    # Build the hash table of the right relation
    right_hash = probe_side_hash_map(right_relation, right_columns)

    for h, right_rows in right_hash.hash_table.items():
        left_rows = left_hash.get(h)
        if not left_rows:
            continue
        for l in left_rows:
            seen_left_rows.add(l)
            left_indexes.extend([l] * len(right_rows))
            right_indexes.extend(right_rows)

    # Yield matching rows
    if left_indexes.size() > 0:
        yield align_tables(
            right_relation,
            left_relation,
            right_indexes.to_numpy(),
            left_indexes.to_numpy(),
        )

    # Emit unmatched left rows using null-filled right columns
    all_left = set(range(left_relation.num_rows))
    unmatched = sorted(all_left - seen_left_rows)

    if unmatched:
        unmatched_left = left_relation.take(pyarrow.array(unmatched))
        # Create a right-side table with zero rows, we do this because
        # we want arrow to do the heavy lifting of adding new columns to
        # the left relation, we do not want to add rows to the left
        # relation - arrow is faster at adding null columns that we can be.
        null_right = pyarrow.table(
            [pyarrow.nulls(0, type=field.type) for field in right_relation.schema],
            schema=right_relation.schema,
        )
        yield pyarrow.concat_tables([unmatched_left, null_right], promote_options="permissive")

    return


def full_join(
    left_relation, right_relation, left_columns: List[str], right_columns: List[str], **kwargs
):
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

    for i in range(0, len(left_indexes), CHUNK_SIZE):
        chunk_left_indexes = left_indexes[i : i + CHUNK_SIZE]
        chunk_right_indexes = right_indexes[i : i + CHUNK_SIZE]

        # Align this chunk and add the resulting table to our list
        yield align_tables(right_relation, left_relation, chunk_right_indexes, chunk_left_indexes)


def right_join(
    left_relation, right_relation, left_columns: List[str], right_columns: List[str], **kwargs
):
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

    hash_table = FlatHashMap()
    non_null_left_values = left_relation.select(left_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_left_values)):
        hash_table.insert(hash(value_tuple), i)

    # Iterate over the right_relation in chunks

    for right_chunk in right_relation.to_batches(CHUNK_SIZE):
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
        self.left_buffer_columns = None
        self.right_buffer = []
        self.left_relation = None
        self.empty_right_relation = None
        self.left_hash = None
        self.left_seen_rows = set()

        self.filter_index = None

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
                if self.join_type == "left outer":
                    start = time.monotonic_ns()
                    self.left_hash = build_side_hash_map(self.left_relation, self.left_columns)
                    self.statistics.time_build_hash_map += time.monotonic_ns() - start

                    if self.left_relation.num_rows < 16_000_001:
                        start = time.monotonic_ns()
                        self.filter_index = create_bloom_filter(
                            self.left_relation, self.left_columns
                        )
                        self.statistics.time_build_bloom_filter += time.monotonic_ns() - start
                        self.statistics.feature_bloom_filter += 1
            else:
                if self.left_buffer_columns is None:
                    self.left_buffer_columns = morsel.schema.names
                else:
                    morsel = morsel.select(self.left_buffer_columns)
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
                    left_hash=self.left_hash,
                    filter_index=self.filter_index,
                )
                yield EOS

            else:
                self.right_buffer.append(morsel)
                yield None


providers = {"left outer": left_join, "full outer": full_join, "right outer": right_join}
