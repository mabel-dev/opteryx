# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Inner Join Node

This is a SQL Query Execution Plan Node.

This is an implementation of a hash join for the INNER JOIN operation in Opteryx. This is a
native implementation that does not use PyArrow's join capabilities. We heavily rely on
Cython to ensure performance.

This implementation includes the use of a bloom filter to quickly eliminate rows.

This implementation doesn't suffer from the limitations of PyArrow's join, such as the
inability to join on STRUCT or ARRAY columns.
"""

import time
from threading import Lock

import pyarrow
from pyarrow import Table

from opteryx import EOS
from opteryx.compiled.joins.inner_join import build_side_hash_map
from opteryx.compiled.joins.inner_join import probe_side_hash_map
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.compiled.structures.buffers import IntBuffer
from opteryx.models import QueryProperties
from opteryx.utils.arrow import align_tables

from . import JoinNode


def inner_join_with_preprocessed_left_side(left_relation, right_relation, join_columns, hash_table):
    """
    Perform an INNER JOIN using a preprocessed hash table from the left relation.

    Parameters:
        left_relation: The preprocessed left pyarrow.Table.
        right_relation: The right pyarrow.Table to join.
        join_columns: A list of column names to join on.
        hash_table: The preprocessed hash table from the left table.

    Returns:
        A tuple containing lists of matching row indices from the left and right relations.
    """
    left_indexes = IntBuffer()
    right_indexes = IntBuffer()

    right_hash = probe_side_hash_map(right_relation, join_columns)
    #    right_hash = build_side_hash_map(right_relation, join_columns)

    for h, right_rows in right_hash.hash_table.items():
        #    for key in right_hash.keys():
        left_rows = hash_table.get(h)
        #        left_rows = hash_table.get(key)
        if left_rows is None:
            continue
        for l in left_rows:
            #            right_rows = right_hash[key]
            left_indexes.extend([l] * len(right_rows))
            right_indexes.extend(right_rows)

    return align_tables(
        right_relation, left_relation, right_indexes.to_numpy(), left_indexes.to_numpy()
    )


class InnerJoinNode(JoinNode):
    join_type = "inner"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_columns = parameters.get("left_columns")
        self.left_relation = None

        self.right_columns = parameters.get("right_columns")

        self.left_buffer = []
        self.left_buffer_columns = None
        self.left_hash = None
        self.left_filter = None

        self.lock = Lock()

    @property
    def name(self):  # pragma: no cover
        return "Inner Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel: Table, join_leg: str) -> Table:
        with self.lock:
            if join_leg == "left":
                if morsel == EOS:
                    self.left_relation = pyarrow.concat_tables(
                        self.left_buffer, promote_options="none"
                    )
                    self.left_buffer.clear()

                    start = time.monotonic_ns()
                    self.left_hash = build_side_hash_map(self.left_relation, self.left_columns)
                    self.statistics.time_inner_join_build_side_hash_map += (
                        time.monotonic_ns() - start
                    )

                    # If the left side is small enough to quickly build a bloom filter, do that.
                    # - We use 1m + 1 as the upper limit to catch LIMIT on 1m rows
                    # The bloom filter has a 16m variation coded, but so far it's not fast enough.
                    if self.left_relation.num_rows < 16_000_001:
                        start = time.monotonic_ns()
                        self.left_filter = create_bloom_filter(
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
                    yield EOS
                    return

                if self.left_filter is not None:
                    # Filter the morsel using the bloom filter, it's a quick way to
                    # reduce the number of rows that need to be joined.
                    start = time.monotonic_ns()
                    maybe_in_left = self.left_filter.possibly_contains_many(
                        morsel, self.right_columns
                    )
                    self.statistics.time_bloom_filtering += time.monotonic_ns() - start
                    morsel = morsel.filter(maybe_in_left)

                    # If the bloom filter is not effective, disable it.
                    # In basic benchmarks, the bloom filter is ~20x the speed of the join.
                    # so the break-even point is about 5% of the rows being eliminated.
                    eliminated_rows = len(maybe_in_left) - morsel.num_rows
                    if eliminated_rows < 0.05 * len(maybe_in_left):
                        self.left_filter = None
                        self.statistics.feature_dynamically_disabled_bloom_filter += 1

                    self.statistics.rows_eliminated_by_bloom_filter += eliminated_rows

                # do the join
                new_morsel = inner_join_with_preprocessed_left_side(
                    left_relation=self.left_relation,
                    right_relation=morsel,
                    join_columns=self.right_columns,
                    hash_table=self.left_hash,
                )

                yield new_morsel
