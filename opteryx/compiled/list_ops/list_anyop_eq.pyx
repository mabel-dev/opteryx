# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import pyarrow
from pyarrow import array as arrow_array
import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int32_t, uint8_t, uintptr_t
from cpython.bytes cimport PyBytes_AsString

cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)


cpdef uint8_t[::1] list_anyop_eq(object literal, object column):
    """
    Arrow-native equivalent of 'val = ANY(array)' for a column of List<Binary> or List<String>.
    Returns a uint8 mask indicating which rows contain the literal.

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
            chunk_view = _anyop_eq_string_chunk(literal, chunk)
        elif pyarrow.types.is_integer(element_type) or pyarrow.types.is_floating(element_type):
            chunk_view = _anyop_eq_primitive_chunk(literal, chunk)
        elif pyarrow.types.is_boolean(element_type):
            chunk_view = _anyop_eq_boolean_chunk(literal, chunk)
        else:
            chunk_view = _anyop_eq_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view


cdef uint8_t[::1] _anyop_eq_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp. Optimized for hot path.
    """
    cdef bytes literal_bytes
    if isinstance(literal, bytes):
        literal_bytes = literal
    else:
        literal_bytes = literal.encode('utf-8')

    cdef const char* literal_ptr = PyBytes_AsString(literal_bytes)
    cdef size_t literal_len = len(literal_bytes)
    cdef list buffers = list_array.buffers()
    cdef const uint8_t* outer_validity = NULL
    cdef const int32_t* offsets = NULL
    cdef Py_ssize_t arr_offset = list_array.offset
    cdef Py_ssize_t row_count = len(list_array)
    cdef Py_ssize_t offset_in_bits = arr_offset & 7
    cdef Py_ssize_t offset_in_bytes = arr_offset >> 3
    cdef object values_array = list_array.values
    cdef list value_buffers = values_array.buffers()
    cdef const uint8_t* inner_validity = NULL
    cdef const int32_t* value_offsets = NULL
    cdef const char* value_data = NULL
    cdef Py_ssize_t i, j, val_start, val_end, val_len
    cdef Py_ssize_t outer_start, outer_end
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(row_count, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result

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
        # Validity check for the outer list
        if outer_validity is not NULL:
            if not (outer_validity[offset_in_bytes + ((offset_in_bits + i) >> 3)] &
                    (1 << ((offset_in_bits + i) & 7))):
                continue  # Null row → result[i] remains 0

        # Row-level offsets into the flat value array
        outer_start = offsets[arr_offset + i]
        outer_end = offsets[arr_offset + i + 1]

        for j in range(outer_start, outer_end):
            # Optionally skip nulls in the inner values
            if inner_validity is not NULL:
                if not (inner_validity[j >> 3] & (1 << (j & 7))):
                    continue

            val_start = value_offsets[j]
            val_end = value_offsets[j + 1]
            val_len = val_end - val_start

            if val_len != literal_len:
                continue

            if memcmp(value_data + val_start, literal_ptr, literal_len) == 0:
                result_view[i] = 1
                break  # short-circuit

    return result_view


cdef uint8_t[::1] _anyop_eq_primitive_chunk(object literal, object list_array):
    """
    Compare each element in a List<Primitive> to `literal`, using raw buffer access. Optimized for hot path.
    Returns a uint8 array where 1 = row contains match, 0 = no match/null.
    """
    cdef list buffers = list_array.buffers()
    cdef const uint8_t* outer_validity = NULL
    cdef const int32_t* offsets = NULL
    cdef Py_ssize_t arr_offset = list_array.offset
    cdef Py_ssize_t row_count = len(list_array)
    cdef Py_ssize_t offset_in_bits = arr_offset & 7
    cdef Py_ssize_t offset_in_bytes = arr_offset >> 3
    cdef object values_array = list_array.values
    cdef list value_buffers = values_array.buffers()
    cdef const uint8_t* inner_validity = NULL
    cdef const char* data = NULL
    cdef Py_ssize_t type_size = values_array.type.bit_width // 8
    cdef Py_ssize_t i, j, outer_start, outer_end
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(row_count, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result

    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address
    if len(value_buffers) > 0 and value_buffers[0]:
        inner_validity = <const uint8_t*> <uintptr_t> value_buffers[0].address
    if len(value_buffers) > 1 and value_buffers[1]:
        data = <const char*> <uintptr_t> value_buffers[1].address

    # Convert Python literal to bytes
    literal_bytes = arrow_array([literal], type=values_array.type).to_numpy().tobytes()
    cdef const char* literal_ptr = <const char*> literal_bytes

    for i in range(row_count):
        if outer_validity is not NULL:
            if not (outer_validity[offset_in_bytes + ((offset_in_bits + i) >> 3)] &
                    (1 << ((offset_in_bits + i) & 7))):
                continue  # null row

        outer_start = offsets[arr_offset + i]
        outer_end = offsets[arr_offset + i + 1]

        for j in range(outer_start, outer_end):
            if inner_validity is not NULL:
                if not (inner_validity[j >> 3] & (1 << (j & 7))):
                    continue  # null element

            if memcmp(data + (j * type_size), literal_ptr, type_size) == 0:
                result_view[i] = 1
                break

    return result_view


cdef uint8_t[::1] _anyop_eq_boolean_chunk(object literal, object list_array):
    """
    Compare each element in a List<Boolean> to `literal`. Optimized for hot path.
    Returns a uint8 array where 1 = row contains match, 0 = no match/null.
    """
    cdef list buffers = list_array.buffers()
    cdef const uint8_t* outer_validity = NULL
    cdef const int32_t* offsets = NULL
    cdef Py_ssize_t arr_offset = list_array.offset
    cdef Py_ssize_t row_count = len(list_array)
    cdef Py_ssize_t offset_in_bits = arr_offset & 7
    cdef Py_ssize_t offset_in_bytes = arr_offset >> 3
    cdef object values_array = list_array.values
    cdef list value_buffers = values_array.buffers()
    cdef const uint8_t* inner_validity = NULL
    cdef const uint8_t* inner_values = NULL
    cdef Py_ssize_t i, j, outer_start, outer_end, byte_index, bit_index
    cdef uint8_t literal_val = bool(literal)
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(row_count, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result

    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address
    if len(value_buffers) > 0 and value_buffers[0]:
        inner_validity = <const uint8_t*> <uintptr_t> value_buffers[0].address
    if len(value_buffers) > 1 and value_buffers[1]:
        inner_values = <const uint8_t*> <uintptr_t> value_buffers[1].address

    for i in range(row_count):
        if outer_validity is not NULL:
            if not (outer_validity[offset_in_bytes + ((offset_in_bits + i) >> 3)] &
                    (1 << ((offset_in_bits + i) & 7))):
                continue

        outer_start = offsets[arr_offset + i]
        outer_end = offsets[arr_offset + i + 1]

        for j in range(outer_start, outer_end):
            if inner_validity is not NULL:
                if not (inner_validity[j >> 3] & (1 << (j & 7))):
                    continue

            byte_index = j >> 3
            bit_index = j & 7
            value_bit = (inner_values[byte_index] >> bit_index) & 1

            if value_bit == literal_val:
                result_view[i] = 1
                break

    return result_view


cdef uint8_t[::1] _anyop_eq_generic_chunk(object literal, object array):
    """
    Generic fallback: compare each value in the array to `literal` using Python equality.
    """
    cdef:
        Py_ssize_t row_count = len(array)
        Py_ssize_t offset = array.offset
        Py_ssize_t i
        object value
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

    if hasattr(literal, "item"):
        literal = literal.item()
    if hasattr(array, "to_pylist"):
        array = array.to_pylist()

    for i in range(row_count):
        value = array[i + offset]
        if value is not None and literal in value:
            result_view[i] = 1

    return result_view
