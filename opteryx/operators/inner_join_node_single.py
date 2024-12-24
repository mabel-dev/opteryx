# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Inner Join Node (Single Condition)

This is a SQL Query Execution Plan Node.

We have a generic Inner Join node, this is optimized for single conditions in the
Inner Join, this is currently only used for INTEGERS and is about 25% faster than
the generic INNER JOIN.
"""

import numpy
import pyarrow
from pyarrow import compute

from opteryx import EOS
from opteryx.compiled.structures import HashTable
from opteryx.models import QueryProperties
from opteryx.utils.arrow import align_tables

from . import JoinNode


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

    elif pyarrow.types.is_fixed_size_binary(array.type) or pyarrow.types.is_floating(
        array.type
    ):  # pragma: no cover
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

    elif pyarrow.types.is_fixed_size_binary(array.type) or pyarrow.types.is_floating(
        array.type
    ):  # pragma: no cover
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


class InnerJoinSingleNode(JoinNode):
    join_type = "inner"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

        self.left_buffer = []
        self.left_hash = None
        self.left_relation = None

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Inner Join (Single)"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel: pyarrow.Table, join_leg: str) -> pyarrow.Table:
        if join_leg == "left":
            if morsel == EOS:
                self.left_relation = pyarrow.concat_tables(self.left_buffer, promote_options="none")
                self.left_buffer.clear()
                self.left_hash = preprocess_left(self.left_relation, self.left_columns)
            else:
                self.left_buffer.append(morsel)
            yield None
            return

        if morsel == EOS:
            yield EOS
            return

        # do the join
        new_morsel = inner_join_with_preprocessed_left_side(
            left_relation=self.left_relation,
            right_relation=morsel,
            join_columns=self.right_columns,
            hash_table=self.left_hash,
        )

        yield new_morsel
