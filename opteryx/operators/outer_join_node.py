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

Performance Optimizations (LEFT OUTER JOIN):
- Streaming processing: Right relation is processed in morsels instead of being fully buffered
- Memory efficiency: Reduced memory footprint by avoiding full right relation buffering
- Cython optimization: Uses optimized Cython implementation with C-level memory management
- Numpy arrays: Uses numpy for faster seen_flags tracking vs Python arrays
- Bloom filters: Pre-filters right relation to quickly eliminate non-matching rows
- Early termination: Tracks matched left rows to enable potential short-circuits
"""

import time
from typing import List

import pyarrow

from opteryx import EOS
from opteryx.compiled.joins import build_side_hash_map
from opteryx.compiled.joins import left_join_optimized
from opteryx.compiled.joins import probe_side_hash_map
from opteryx.compiled.joins import right_join
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.compiled.structures.buffers import IntBuffer
from opteryx.compiled.structures.hash_table import HashTable
from opteryx.models import QueryProperties
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
    
    This implementation is optimized for performance by:
    1. Using Cython-optimized IntBuffer for index tracking
    2. Using numpy array for seen_flags (faster than Python array)
    3. Early termination when all left rows are matched
    4. Efficient bloom filter pre-filtering
    
    Yields:
        pyarrow.Table chunks of the joined result.
    """
    import numpy

    left_indexes = IntBuffer()
    right_indexes = IntBuffer()
    # Use numpy array instead of Python array for better performance
    seen_flags = numpy.zeros(left_relation.num_rows, dtype=numpy.uint8)

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

    # Track number of matched left rows for early termination
    matched_count = 0
    total_left_rows = left_relation.num_rows

    for h, right_rows in right_hash.hash_table.items():
        left_rows = left_hash.get(h)
        if not left_rows:
            continue
        for l in left_rows:
            if seen_flags[l] == 0:
                seen_flags[l] = 1
                matched_count += 1
            left_indexes.extend([l] * len(right_rows))
            right_indexes.extend(right_rows)
        
        # Early termination: if all left rows are matched, no need to continue
        if matched_count == total_left_rows:
            break

    # Yield matching rows
    if left_indexes.size() > 0:
        yield align_tables(
            right_relation,
            left_relation,
            right_indexes.to_numpy(),
            left_indexes.to_numpy(),
        )

    # Only process unmatched rows if we didn't match everything
    if matched_count < total_left_rows:
        # Use numpy where for faster array filtering
        unmatched = numpy.where(seen_flags == 0)[0]

        if len(unmatched) > 0:
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
        hash_table.insert(abs(hash(value_tuple)), i)

    left_indexes = []
    right_indexes = []

    left_values = left_relation.select(left_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*left_values)):
        rows = hash_table.get(abs(hash(value_tuple)))
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
        self.right_schema = None  # Store right relation schema for streaming
        self.left_relation = None
        self.empty_right_relation = None
        self.left_hash = None
        self.left_seen_flags = None  # numpy array for tracking matched rows (streaming)
        self.matched_count = 0  # Track how many left rows have been matched (streaming)

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
                import numpy
                self.left_relation = pyarrow.concat_tables(self.left_buffer, promote_options="none")
                self.left_buffer.clear()
                if self.join_type == "left outer":
                    start = time.monotonic_ns()
                    self.left_hash = build_side_hash_map(self.left_relation, self.left_columns)
                    self.statistics.time_build_hash_map += time.monotonic_ns() - start

                    # Initialize seen_flags array for tracking matched rows (streaming)
                    self.left_seen_flags = numpy.zeros(self.left_relation.num_rows, dtype=numpy.uint8)

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
                # For non-left outer joins, use the original buffering approach
                if self.join_type != "left outer":
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
                else:
                    # For left outer join, emit unmatched left rows after all right data is processed
                    import numpy
                    
                    # Only process unmatched rows if we didn't match everything
                    if self.matched_count < self.left_relation.num_rows:
                        unmatched = numpy.where(self.left_seen_flags == 0)[0]
                        
                        if len(unmatched) > 0 and self.right_schema is not None:
                            unmatched_left = self.left_relation.take(pyarrow.array(unmatched))
                            # Create a right-side table with zero rows using the stored schema
                            null_right = pyarrow.table(
                                [pyarrow.nulls(0, type=field.type) for field in self.right_schema],
                                schema=self.right_schema,
                            )
                            yield pyarrow.concat_tables([unmatched_left, null_right], promote_options="permissive")
                
                yield EOS

            else:
                # For left outer join, process right morsels as they arrive (streaming)
                if self.join_type == "left outer":
                    yield from self._process_left_outer_join_morsel(morsel)
                else:
                    # For other join types, buffer the right relation
                    self.right_buffer.append(morsel)
                    yield None
    
    def _process_left_outer_join_morsel(self, morsel: pyarrow.Table):
        """
        Process a single right-side morsel for left outer join.
        This enables streaming processing instead of buffering all right data.
        """
        # Store schema from first morsel for later use when emitting unmatched rows
        if self.right_schema is None:
            self.right_schema = morsel.schema
        
        # Apply bloom filter if available
        if self.filter_index:
            possibly_matching_rows = self.filter_index.possibly_contains_many(morsel, self.right_columns)
            morsel = morsel.filter(possibly_matching_rows)
            
            # If no matches after filtering, skip this morsel
            if morsel.num_rows == 0:
                yield None
                return
        
        # Build hash map for this right morsel
        right_hash = probe_side_hash_map(morsel, self.right_columns)
        
        left_indexes = IntBuffer()
        right_indexes = IntBuffer()
        
        # Find matching rows
        for h, right_rows in right_hash.hash_table.items():
            left_rows = self.left_hash.get(h)
            if not left_rows:
                continue
            for l in left_rows:
                # Mark this left row as seen (only count once)
                if self.left_seen_flags[l] == 0:
                    self.left_seen_flags[l] = 1
                    self.matched_count += 1
                left_indexes.extend([l] * len(right_rows))
                right_indexes.extend(right_rows)
        
        # Yield matching rows if any
        if left_indexes.size() > 0:
            yield align_tables(
                morsel,
                self.left_relation,
                right_indexes.to_numpy(),
                left_indexes.to_numpy(),
            )
        else:
            yield None


providers = {"left outer": left_join, "full outer": full_join, "right outer": right_join}
