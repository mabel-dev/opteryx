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
Inner Join Node

This is a SQL Query Execution Plan Node.

PyArrow has a good LEFT JOIN implementation, but it errors when the
relations being joined contain STRUCT or ARRAY columns, this is true
for all of the JOIN types, however we've only written our own INNER
and LEFT JOINs.

It is comparible performance to the PyArrow INNER JOIN, in benchmarks
sometimes native is faster, sometimes PyArrow is faster. Generally
PyArrow is more forgiving when the relations are the "wrong" way around
(unoptimized order) but native is faster for well-ordered relations, as
we intend to take steps to help ensure relations are well-ordered, this
should work in our favour.

This is a hash join, this is completely rewritten from the earlier
pyarrow_ops implementation which was a variation of a sort-merge join.
"""
import time
from typing import Generator

import numpy
import pyarrow

from opteryx.compiled.structures import HashTable
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.utils.arrow import align_tables


def preprocess_left(relation, join_columns):
    """
    Build a hash table for the left side of the join operation.

    Parameters:
        relation: The left pyarrow.Table to preprocess.
        join_columns: A list of column names to join on.

    Returns:
        A HashTable where keys are hashes of the join column entries and
        values are lists of row indices corresponding to each hash key.
    """
    ht = HashTable()
    values = relation.select(join_columns)
    num_rows = values.num_rows

    # Prepare to track nulls across all relevant columns
    combined_nulls = numpy.zeros(num_rows, dtype=bool)

    # Process each column to update the combined null bitmap
    for column_name in join_columns:
        column = values.column(column_name)
        if column.null_count > 0:
            # Get the null bitmap for the current column, ensure it's in a single chunk first
            bitmap_buffer = column.combine_chunks().buffers()[0]
            if bitmap_buffer is not None:
                # Update combined nulls bitmap
                bitmap_buffer = numpy.bitwise_not(bitmap_buffer)
                combined_nulls = numpy.logical_or(
                    combined_nulls,
                    [((byte >> bit) & 1) for byte in bitmap_buffer for bit in range(8)][:num_rows],
                )

    # Determine row indices that have nulls in any of the considered columns
    null_indices = numpy.nonzero(combined_nulls)[0]

    # Create a set for faster lookup
    null_indices_set = set(null_indices)

    # Process values to insert into the hash table
    for i, value_tuple in enumerate(zip(*values.itercolumns())):
        if i not in null_indices_set:
            ht.insert(hash(value_tuple), i)

    return ht


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
    left_indexes = []
    right_indexes = []

    right_values = right_relation.select(join_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*right_values)):
        rows = hash_table.get(hash(value_tuple))
        if rows:
            left_indexes.extend(rows)
            right_indexes.extend([i] * len(rows))

    return align_tables(right_relation, left_relation, right_indexes, left_indexes)


class InnerJoinNode(BasePlanNode):

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
        return "Inner Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        left_relation = pyarrow.concat_tables(left_node.execute(), promote_options="none")
        # in place until #1295 resolved
        if not self._left_columns[0] in left_relation.column_names:
            self._right_columns, self._left_columns = self._left_columns, self._right_columns

        start = time.monotonic_ns()
        left_hash = preprocess_left(left_relation, self._left_columns)
        self.statistics.time_inner_join += time.monotonic_ns() - start
        for morsel in right_node.execute():
            start = time.monotonic_ns()
            # do the join
            new_morsel = inner_join_with_preprocessed_left_side(
                left_relation=left_relation,
                right_relation=morsel,
                join_columns=self._right_columns,
                hash_table=left_hash,
            )
            self.statistics.time_inner_join += time.monotonic_ns() - start
            yield new_morsel
