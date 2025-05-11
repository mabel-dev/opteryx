# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import pyarrow
import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int32_t, uint8_t, uintptr_t
from cpython.bytes cimport PyBytes_AsString

cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)

cpdef uint8_t[::1] list_allop_eq(object literal, object column):
    """
    Arrow-native equivalent of 'val = ALL(array)' for a column of List<Binary> or List<String>.
    Returns a uint8 mask indicating which rows match the literal across all non-null elements,
    with no nulls in the list (SQL-compatible semantics).

    Parameters:
        literal: object
            A string or bytes to compare against.
        column: object
            An Arrow ListArray or ChunkedArray of List<Binary> or List<String>.

    Returns:
        uint8_t[::1]: mask of matching rows.
    """
    cdef:
        list chunks = column if isinstance(column, list) else (column.chunks if hasattr(column, "chunks") else [column])
        Py_ssize_t total_length = 0
        numpy.ndarray[numpy.uint8_t, ndim=1] final_result
        uint8_t[::1] final_view
        Py_ssize_t offset = 0
        uint8_t[::1] chunk_view
        object chunk

    for chunk in chunks:
        total_length += len(chunk)

    final_result = numpy.zeros(total_length, dtype=numpy.uint8)
    final_view = final_result

    offset = 0
    for chunk in chunks:

        column_type = chunk.type
        if not pyarrow.types.is_list(column_type):
            raise TypeError("Expected ListArray or ChunkedArray of lists")
        element_type = column_type.value_type

        if pyarrow.types.is_string(element_type) or pyarrow.types.is_binary(element_type):
            chunk_view = _allop_eq_string_chunk(literal, chunk)
        else:
            chunk_view = _allop_eq_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view

cdef uint8_t[::1] _allop_eq_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp.
    Follows SQL semantics: NULL == X → UNKNOWN → row = FALSE
    """
    cdef:
        bytes literal_bytes = b'' if literal is None else literal.encode('utf-8')
        const char* literal_ptr = PyBytes_AsString(literal_bytes)
        size_t literal_len = len(literal_bytes)

        list buffers = list_array.buffers()
        const uint8_t* outer_validity = NULL
        const int32_t* offsets = NULL
        Py_ssize_t arr_offset = list_array.offset
        Py_ssize_t row_count = len(list_array)

        Py_ssize_t offset_in_bits = arr_offset & 7
        Py_ssize_t offset_in_bytes = arr_offset >> 3

        object values_array = list_array.values
        list value_buffers = values_array.buffers()
        const uint8_t* inner_validity = NULL
        const int32_t* value_offsets = NULL
        const char* value_data = NULL

        Py_ssize_t i, j, val_start, val_end, val_len
        Py_ssize_t outer_start, outer_end

        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    if len(value_buffers) > 0 and value_buffers[0]:
        inner_validity = <const uint8_t*> <uintptr_t> value_buffers[0].address
    if len(value_buffers) > 1 and value_buffers[1]:
        value_offsets = <const int32_t*> <uintptr_t> value_buffers[1].address
    if len(value_buffers) > 2 and value_buffers[2]:
        value_data = <const char*> <uintptr_t> value_buffers[2].address

    for i in range(row_count):
        if outer_validity is not NULL:
            if not (outer_validity[offset_in_bytes + ((offset_in_bits + i) >> 3)] &
                    (1 << ((offset_in_bits + i) & 7))):
                continue  # null row → stays 0

        outer_start = offsets[arr_offset + i]
        outer_end = offsets[arr_offset + i + 1]

        if outer_end == outer_start:
            continue  # empty list → ALL EQ is false

        for j in range(outer_start, outer_end):
            if inner_validity is not NULL:
                if not (inner_validity[j >> 3] & (1 << (j & 7))):
                    break  # found NULL → row is FALSE

            val_start = value_offsets[j]
            val_end = value_offsets[j + 1]
            val_len = val_end - val_start

            if val_len != literal_len or memcmp(value_data + val_start, literal_ptr, literal_len) != 0:
                break  # found non-match → row is FALSE
        else:
            result_view[i] = 1  # ALL matched

    return result_view

cdef uint8_t[::1] _allop_eq_generic_chunk(object literal, object array):
    """
    Fallback implementation for non-string lists using Python equality.
    SQL semantics: nulls = unknowns = FALSE for ALL
    """
    cdef:
        Py_ssize_t row_count = len(array)
        Py_ssize_t offset = array.offset
        Py_ssize_t i
        object value, item
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

    if hasattr(literal, "item"):
        literal = literal.item()
    if hasattr(array, "to_pylist"):
        array = array.to_pylist()

    for i in range(row_count):
        value = array[i + offset]

        if value is None or len(value) == 0 or None in value:
            continue  # Null list or contains null → UNKNOWN → result = 0

        for item in value:
            if item != literal:
                break  # mismatch
        else:
            result_view[i] = 1  # All matched

    return result_view
