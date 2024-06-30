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
Inner Join Node (Single Condition)

This is a SQL Query Execution Plan Node.

We have a generic Inner Join node, this is optimized for single conditions in the
Inner Join, this is currently only used for INTEGERS and is about 25% faster than
the generic INNER JOIN.
"""

import time
from typing import Generator

import numpy
import pyarrow
from pyarrow import compute

from opteryx.compiled.structures import HashTable
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType
from opteryx.utils.arrow import align_tables


def preprocess_left(relation, join_columns):
    """
    Convert a PyArrow array to an array of bytes.

    Parameters:
        array (pyarrow.Array): The input PyArrow array.

    Returns:
        numpy.ndarray: A numpy array of bytes representing the values in the input array.
    """
    ht = HashTable()

    array = relation.column(join_columns[0])

    if isinstance(array, pyarrow.ChunkedArray):
        array = array.combine_chunks()

    num_rows = len(array)
    # Access the null bitmap buffer
    null_bitmap = array.buffers()[0]

    if null_bitmap is not None:
        null_array = [((byte >> bit) & 1) for byte in null_bitmap for bit in range(8)][:num_rows]
    else:
        null_array = numpy.ones(num_rows, dtype=bool)

    value_offset_map = numpy.where(null_array)[0]
    non_null_array = array.filter(compute.is_valid(array))

    if pyarrow.types.is_integer(array.type):
        for i, val in enumerate(non_null_array.to_numpy()):
            ht.insert(val, value_offset_map[i])

    elif pyarrow.types.is_fixed_size_binary(array.type) or pyarrow.types.is_floating(array.type):
        # Access the data buffer directly for fixed-width types
        data_buffer = array.buffers()[1]
        item_size = array.type.bit_width // 8

        for i in range(num_rows):
            if null_array[i]:
                start = i * item_size
                end = start + item_size
                value_bytes = data_buffer[start:end].to_pybytes()
                ht.insert(hash(value_bytes), i)

    elif pyarrow.types.is_binary(array.type) or pyarrow.types.is_string(array.type):
        for i, val in enumerate(array):
            if null_array[i]:
                ht.insert(hash(val), i)

    else:
        raise TypeError(f"Unsupported column type: {array.type}")

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

    array = right_relation.column(join_columns[0])

    if isinstance(array, pyarrow.ChunkedArray):
        array = array.combine_chunks()

    num_rows = len(array)
    # Access the null bitmap buffer
    null_bitmap = array.buffers()[0]

    if null_bitmap is not None:
        null_array = [((byte >> bit) & 1) for byte in null_bitmap for bit in range(8)][:num_rows]
    else:
        null_array = numpy.ones(num_rows, dtype=bool)

    value_offset_map = numpy.where(null_array)[0]
    non_null_array = array.filter(compute.is_valid(array))

    if pyarrow.types.is_integer(array.type):
        for i, val in enumerate(non_null_array.to_numpy()):
            rows = hash_table.get(val)
            if rows:
                left_indexes.extend(rows)
                right_indexes.extend([value_offset_map[i]] * len(rows))

    elif pyarrow.types.is_fixed_size_binary(array.type) or pyarrow.types.is_floating(array.type):
        # Access the data buffer directly for fixed-width types
        data_buffer = array.buffers()[1]
        item_size = array.type.bit_width // 8

        for i in range(num_rows):
            if null_array[i]:
                start = i * item_size
                end = start + item_size
                value_bytes = data_buffer[start:end].to_pybytes()
                rows = hash_table.get(hash(value_bytes))
                if rows:
                    left_indexes.extend(rows)
                    right_indexes.extend([i] * len(rows))

    if pyarrow.types.is_binary(array.type) or pyarrow.types.is_string(array.type):
        for i, val in enumerate(array):
            if null_array[i]:
                rows = hash_table.get(hash(val))
                if rows:
                    left_indexes.extend(rows)
                    right_indexes.extend([i] * len(rows))

    return align_tables(right_relation, left_relation, right_indexes, left_indexes)


class InnerJoinSingleNode(BasePlanNode):
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
        return "Inner Join (Single)"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self) -> Generator:
        left_node = self._producers[0]  # type:ignore
        right_node = self._producers[1]  # type:ignore

        left_relation = pyarrow.concat_tables(left_node.execute(), promote_options="none")
        # in place until #1295 resolved
        if self._left_columns[0] not in left_relation.column_names:
            self._right_columns, self._left_columns = (
                self._left_columns,
                self._right_columns,
            )

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
