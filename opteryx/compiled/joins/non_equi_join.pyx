# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Non-Equi Join Implementation

This module implements non-equi joins using a nested loop algorithm with draken
for efficient columnar comparison operations.

Supports: !=, >, >=, <, <=
"""

import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int64_t

from opteryx.compiled.structures.buffers cimport IntBuffer


cpdef tuple non_equi_nested_loop_join(
    left_relation,
    right_relation,
    str left_column,
    str right_column,
    str comparison_op
):
    """
    Perform a nested loop join for non-equi comparisons using draken.

    This is a simple, unoptimized implementation that converts Arrow tables to
    draken representation and uses vector comparison operations.

    Args:
        left_relation: Arrow table for left side
        right_relation: Arrow table for right side
        left_column: Column name from left table
        right_column: Column name from right table
        comparison_op: One of 'not_equals', 'greater_than', 'greater_than_or_equals',
                      'less_than', 'less_than_or_equals'

    Returns:
        Tuple of (left_indices, right_indices) as numpy arrays
    """
    from opteryx.draken.morsels.morsel import Morsel

    # Convert Arrow tables to draken morsels
    cdef object left_morsel = Morsel.from_arrow(left_relation)
    cdef object right_morsel = Morsel.from_arrow(right_relation)

    cdef int64_t left_rows = left_relation.num_rows
    cdef int64_t right_rows = right_relation.num_rows

    if left_rows == 0 or right_rows == 0:
        return numpy.empty(0, dtype=numpy.int64), numpy.empty(0, dtype=numpy.int64)

    # Get column vectors
    cdef object left_col_bytes = left_column.encode('utf-8')
    cdef object right_col_bytes = right_column.encode('utf-8')
    cdef object left_vec = left_morsel.column(left_col_bytes)
    cdef object right_vec = right_morsel.column(right_col_bytes)

    # Buffers to collect matching indices
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()

    cdef int64_t i, j
    cdef object left_val, right_val, comparison_result

    # Nested loop join - compare each left row with each right row
    for i in range(left_rows):
        left_val = left_vec[i]
        
        # Skip null values in left
        if left_val is None:
            continue

        for j in range(right_rows):
            right_val = right_vec[j]
            
            # Skip null values in right
            if right_val is None:
                continue

            # Perform the comparison
            if comparison_op == 'not_equals':
                comparison_result = left_val != right_val
            elif comparison_op == 'greater_than':
                comparison_result = left_val > right_val
            elif comparison_op == 'greater_than_or_equals':
                comparison_result = left_val >= right_val
            elif comparison_op == 'less_than':
                comparison_result = left_val < right_val
            elif comparison_op == 'less_than_or_equals':
                comparison_result = left_val <= right_val
            else:
                raise ValueError(f"Unsupported comparison operator: {comparison_op}")

            if comparison_result:
                left_indexes.append(i)
                right_indexes.append(j)

    return left_indexes.to_numpy(), right_indexes.to_numpy()
