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
from opteryx.compiled.joins import build_side_hash_map
from opteryx.compiled.joins import get_last_inner_join_metrics
from opteryx.compiled.joins import inner_join
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.models import QueryProperties
from opteryx.utils.arrow import align_tables

from . import JoinNode


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

        self.columns = parameters.get("columns")

        self.lock = Lock()

    @property
    def name(self):  # pragma: no cover
        return "Inner Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel: Table, join_leg: str) -> Table:
        morsel = self.ensure_arrow_table(morsel)

        with self.lock:
            if join_leg == "left":
                if morsel == EOS:
                    self.left_relation = pyarrow.concat_tables(
                        self.left_buffer, promote_options="none"
                    )
                    self.left_buffer.clear()

                    start = time.monotonic_ns()
                    self.left_hash = build_side_hash_map(self.left_relation, self.left_columns)
                    self.statistics.increase(
                        "time_inner_join_build_side_hash_map",
                        time.monotonic_ns() - start,
                    )

                    # If the left side is small enough to quickly build a bloom filter, do that.
                    # - We use 16m + 1 as the upper limit to catch LIMIT on 16m rows
                    if self.left_relation.num_rows < 16_000_001:
                        start = time.monotonic_ns()
                        self.left_filter = create_bloom_filter(
                            self.left_relation, self.left_columns
                        )
                        self.statistics.increase(
                            "time_build_bloom_filter", time.monotonic_ns() - start
                        )
                        self.statistics.increase("feature_bloom_filter", 1)

                    # Project the left relation down to only the columns we need in the
                    # resulting morsel. This reduces the amount of data we keep in memory
                    # and pass through the align steps.
                    if self.columns is not None:
                        candidates = [c.schema_column.identity for c in self.columns]
                        left_keep = [c for c in candidates if c in self.left_relation.schema.names]
                        if len(left_keep) < len(self.left_relation.schema.names):
                            self.left_relation = self.left_relation.select(left_keep)
                            self.statistics.feature_eliminate_left_join_columns = 1
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
                    self.statistics.increase("time_bloom_filtering", time.monotonic_ns() - start)
                    morsel = morsel.filter(maybe_in_left)

                    # If the bloom filter is not effective, disable it.
                    # In basic benchmarks, the bloom filter is ~20x the speed of the join.
                    # so the break-even point is about 5% of the rows being eliminated.
                    eliminated_rows = len(maybe_in_left) - morsel.num_rows
                    if eliminated_rows < 0.05 * len(maybe_in_left):
                        self.left_filter = None
                        self.statistics.increase("feature_dynamically_disabled_bloom_filter", 1)

                    self.statistics.increase("rows_eliminated_by_bloom_filter", eliminated_rows)

                # do the join
                left_indicies, right_indicies = inner_join(
                    morsel, self.right_columns, self.left_hash
                )

                # record detailed timing and row counts for diagnostics
                (
                    hash_time,
                    probe_time,
                    rows_hashed,
                    candidate_rows,
                    matched_rows,
                    materialize_time,
                ) = get_last_inner_join_metrics()
                self.statistics.increase("time_inner_join_hash", hash_time)
                self.statistics.increase("time_inner_join_probe", probe_time)
                self.statistics.increase("rows_inner_join_hashed", rows_hashed)
                self.statistics.increase("rows_inner_join_candidates", candidate_rows)
                self.statistics.increase("time_inner_join_indices", materialize_time)
                self.statistics.increase("rows_inner_join_matched", matched_rows)
                start = time.monotonic_ns()

                # Project the right relation down to only the columns we need in the
                # resulting morsel. This reduces the amount of data we keep in memory
                # and pass through the align steps.
                if self.columns is not None:
                    candidates = [c.schema_column.identity for c in self.columns]
                    right_keep = [c for c in candidates if c in morsel.schema.names]
                    if len(right_keep) < len(morsel.schema.names):
                        morsel = morsel.select(right_keep)
                        self.statistics.feature_eliminate_right_join_columns = 1

                aligned = align_tables(morsel, self.left_relation, right_indicies, left_indicies)
                self.statistics.increase("time_inner_join_align", time.monotonic_ns() - start)

                yield aligned
