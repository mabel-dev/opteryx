# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False


# Imports
import numpy
cimport numpy
import pyarrow
import platform
from pyarrow import array as arrow_array

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_AsString, PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_AsUTF8String
from cython import Py_ssize_t
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t, uintptr_t

numpy.import_array()


# C/C++ External Declarations
cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)

cdef extern from *:
    """
    #ifdef _WIN32
    #define strncasecmp _strnicmp
    #endif
    """

cdef extern from "simd_search.h":
    int neon_search(const char *data, size_t length, char target)
    int avx_search(const char *data, size_t length, char target)



# Function Implementations


# ===== Functions from list_allop_eq.pyx =====




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

# ===== Functions from list_allop_neq.pyx =====





cpdef uint8_t[::1] list_allop_neq(object literal, object column):
    """
    Arrow-native equivalent of 'val != ANY(array)' for a column of List<Binary> or List<String>.
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
            chunk_view = _allop_neq_string_chunk(literal, chunk)
        else:
            chunk_view = _allop_neq_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view


cdef uint8_t[::1] _allop_neq_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp.
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

        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.ones(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

    # Outer array buffers (list<...>)
    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    # Inner value array buffers (flat binary/string values)
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
                result_view[i] = literal is None
                continue  # Null row → result[i] remains 0

        # Row-level offsets into the flat value array
        outer_start = offsets[arr_offset + i]
        outer_end = offsets[arr_offset + i + 1]

        if outer_end == outer_start:
            result_view[i] = 0
            continue

        for j in range(outer_start, outer_end):
            # NULLs never match, so the condition is None
            if inner_validity is not NULL:
                if not (inner_validity[j >> 3] & (1 << (j & 7))):
                    result_view[i] = 0
                    break  # short-circuit

            val_start = value_offsets[j]
            val_end = value_offsets[j + 1]
            val_len = val_end - val_start

            if val_len != literal_len:
                continue

            if memcmp(value_data + val_start, literal_ptr, literal_len) == 0:
                result_view[i] = 0
                break  # short-circuit

    return result_view


cdef uint8_t[::1] _allop_neq_generic_chunk(object literal, object array):
    """
    Generic fallback: compare each value in the array to `literal` using Python equality.
    """
    cdef:
        Py_ssize_t row_count = len(array)
        Py_ssize_t offset = array.offset
        Py_ssize_t i
        object value
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.ones(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result
        bint literal_is_none = literal is None
        bint literal_is_nan = False

    if hasattr(literal, "item"):
        literal = literal.item()
    if hasattr(array, "to_pylist"):
        array = array.to_pylist()

    # Check if literal is NaN
    if isinstance(literal, float) and numpy.isnan(literal):
        literal_is_nan = True

    for i in range(row_count):
        value = array[i + offset]
        if value is None:
            result_view[i] = literal_is_none
        elif len(value) == 0:
            result_view[i] = 0
        elif None in value:
            result_view[i] = 0
        elif literal_is_nan:
            for j in range(len(value)):
                item = value[j]
                if isinstance(item, float) and numpy.isnan(item):
                    result_view[i] = 0  # Found a NaN in array, not all elements ≠ literal
                    break
        elif literal in value:
            result_view[i] = literal_is_none
        elif literal_is_none:
            result_view[i] = 0

    return result_view

# ===== Functions from list_anyop_eq.pyx =====





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

# ===== Functions from list_anyop_gt.pyx =====





cpdef uint8_t[::1] list_anyop_gt(object literal, object column):
    """
    Arrow-native equivalent of 'val > ANY(array)' for a column of List<Binary> or List<String>.
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
            chunk_view = _anyop_gt_string_chunk(literal, chunk)
        else:
            chunk_view = _anyop_gt_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view

cdef inline bint string_gt(const char* a, Py_ssize_t a_len, const char* b, Py_ssize_t b_len):
    """
    Lexicographic greater-than for UTF-8 strings.
    Equivalent to: a > b in Python
    """
    cdef Py_ssize_t i = 0
    while i < a_len and i < b_len:
        if a[i] != b[i]:
            return a[i] > b[i]
        i += 1
    return a_len > b_len  # If equal up to min len, longer wins

cdef uint8_t[::1] _anyop_gt_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp.
    """
    cdef:
        bytes literal_bytes = literal.encode('utf-8')
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

    # Outer array buffers (list<...>)
    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    # Inner value array buffers (flat binary/string values)
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

            if string_gt(literal_ptr, literal_len, value_data + val_start, val_len):
                result_view[i] = 1
                break  # short-circuit

    return result_view


cdef uint8_t[::1] _anyop_gt_generic_chunk(object literal, object array):
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
        if value is not None:
            for v in value:
                if literal > v:
                    result_view[i] = 1
                    break

    return result_view

# ===== Functions from list_anyop_gte.pyx =====





cpdef uint8_t[::1] list_anyop_gte(object literal, object column):
    """
    Arrow-native equivalent of 'val > ANY(array)' for a column of List<Binary> or List<String>.
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
            chunk_view = _anyop_gte_string_chunk(literal, chunk)
        else:
            chunk_view = _anyop_gte_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view

cdef inline bint string_gte(const char* a, Py_ssize_t a_len, const char* b, Py_ssize_t b_len):
    """
    Lexicographic greater-than for UTF-8 strings.
    Equivalent to: a > b in Python
    """
    cdef Py_ssize_t i = 0
    while i < a_len and i < b_len:
        if a[i] != b[i]:
            return a[i] >= b[i]
        i += 1
    return a_len >= b_len  # If equal up to min len, longer wins

cdef uint8_t[::1] _anyop_gte_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp.
    """
    cdef:
        bytes literal_bytes = literal.encode('utf-8')
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

    # Outer array buffers (list<...>)
    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    # Inner value array buffers (flat binary/string values)
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

            if string_gte(literal_ptr, literal_len, value_data + val_start, val_len):
                result_view[i] = 1
                break  # short-circuit

    return result_view


cdef uint8_t[::1] _anyop_gte_generic_chunk(object literal, object array):
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
        if value is not None:
            for v in value:
                if literal >= v:
                    result_view[i] = 1
                    break

    return result_view

# ===== Functions from list_anyop_lt.pyx =====





cpdef uint8_t[::1] list_anyop_lt(object literal, object column):
    """
    Arrow-native equivalent of 'val > ANY(array)' for a column of List<Binary> or List<String>.
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
            chunk_view = _anyop_lt_string_chunk(literal, chunk)
        else:
            chunk_view = _anyop_lt_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view

cdef inline bint string_lt(const char* a, Py_ssize_t a_len, const char* b, Py_ssize_t b_len):
    """
    Lexicographic greater-than for UTF-8 strings.
    Equivalent to: a > b in Python
    """
    cdef Py_ssize_t i = 0
    while i < a_len and i < b_len:
        if a[i] != b[i]:
            return a[i] < b[i]
        i += 1
    return a_len < b_len  # If equal up to min len, longer wins

cdef uint8_t[::1] _anyop_lt_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp.
    """
    cdef:
        bytes literal_bytes = literal.encode('utf-8')
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

    # Outer array buffers (list<...>)
    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    # Inner value array buffers (flat binary/string values)
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

            if string_lt(literal_ptr, literal_len, value_data + val_start, val_len):
                result_view[i] = 1
                break  # short-circuit

    return result_view


cdef uint8_t[::1] _anyop_lt_generic_chunk(object literal, object array):
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
        if value is not None:
            for v in value:
                if literal < v:
                    result_view[i] = 1
                    break

    return result_view

# ===== Functions from list_anyop_lte.pyx =====





cpdef uint8_t[::1] list_anyop_lte(object literal, object column):
    """
    Arrow-native equivalent of 'val > ANY(array)' for a column of List<Binary> or List<String>.
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
            chunk_view = _anyop_lte_string_chunk(literal, chunk)
        else:
            chunk_view = _anyop_lte_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view

cdef inline bint string_lte(const char* a, Py_ssize_t a_len, const char* b, Py_ssize_t b_len):
    """
    Lexicographic greater-than for UTF-8 strings.
    Equivalent to: a > b in Python
    """
    cdef Py_ssize_t i = 0
    while i < a_len and i < b_len:
        if a[i] != b[i]:
            return a[i] <= b[i]
        i += 1
    return a_len <= b_len  # If equal up to min len, longer wins

cdef uint8_t[::1] _anyop_lte_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp.
    """
    cdef:
        bytes literal_bytes = literal.encode('utf-8')
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

    # Outer array buffers (list<...>)
    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    # Inner value array buffers (flat binary/string values)
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

            if string_lte(literal_ptr, literal_len, value_data + val_start, val_len):
                result_view[i] = 1
                break  # short-circuit

    return result_view


cdef uint8_t[::1] _anyop_lte_generic_chunk(object literal, object array):
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
        if value is not None:
            for v in value:
                if literal <= v:
                    result_view[i] = 1
                    break

    return result_view

# ===== Functions from list_anyop_neq.pyx =====





cpdef uint8_t[::1] list_anyop_neq(object literal, object column):
    """
    Arrow-native equivalent of 'val != ANY(array)' for a column of List<Binary> or List<String>.
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
            chunk_view = _anyop_neq_string_chunk(literal, chunk)
        elif pyarrow.types.is_integer(element_type) or pyarrow.types.is_floating(element_type):
            chunk_view = _anyop_neq_primitive_chunk(literal, chunk)
        elif pyarrow.types.is_boolean(element_type):
            chunk_view = _anyop_neq_boolean_chunk(literal, chunk)
        else:
            chunk_view = _anyop_neq_generic_chunk(literal, chunk)

        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view


cdef uint8_t[::1] _anyop_neq_string_chunk(object literal, object list_array):
    """
    Chunk processor for List<Binary> or List<String>. Uses memcmp.
    """
    cdef literal_bytes

    if isinstance(literal, bytes):
        literal_bytes = literal
    else:
        literal_bytes = literal.encode('utf-8')

    cdef:
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

    # Outer array buffers (list<...>)
    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    # Inner value array buffers (flat binary/string values)
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
                continue  # Null row → result[i] is 0

        # Row-level offsets into the flat value array
        outer_start = offsets[arr_offset + i]
        outer_end = offsets[arr_offset + i + 1]

        for j in range(outer_start, outer_end):
            # Optionally skip nulls in the inner values
            if inner_validity is not NULL:
                if not (inner_validity[j >> 3] & (1 << (j & 7))):
                    result_view[i] = 1
                    continue

            val_start = value_offsets[j]
            val_end = value_offsets[j + 1]
            val_len = val_end - val_start

            # if they're different lengths, they don't match
            if val_len != literal_len:
                result_view[i] = 1
                break  # short-circuit

            if memcmp(value_data + val_start, literal_ptr, literal_len) != 0:
                result_view[i] = 1
                break

    return result_view


cdef uint8_t[::1] _anyop_neq_primitive_chunk(object literal, object list_array):
    """
    Compare each element in a List<Primitive> to `literal`, using raw buffer access.
    Returns a uint8 array where 1 = row contains match, 0 = no match/null.
    """
    cdef:
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
        const char* data = NULL

        Py_ssize_t type_size = values_array.type.bit_width // 8
        Py_ssize_t i, j, outer_start, outer_end
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.zeros(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

    # Outer array buffers
    if len(buffers) > 0 and buffers[0]:
        outer_validity = <const uint8_t*> <uintptr_t> buffers[0].address
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*> <uintptr_t> buffers[1].address

    # Inner value buffers
    if len(value_buffers) > 0 and value_buffers[0]:
        inner_validity = <const uint8_t*> <uintptr_t> value_buffers[0].address
    if len(value_buffers) > 1 and value_buffers[1]:
        data = <const char*> <uintptr_t> value_buffers[1].address

    # Convert Python literal to bytes
    # We'll memcmp using literal_ptr
    literal_bytes = arrow_array([literal], type=values_array.type).to_numpy().tobytes()
    literal_ptr = <const char*> literal_bytes

    for i in range(row_count):
        if outer_validity is not NULL:
            if not (outer_validity[offset_in_bytes + ((offset_in_bits + i) >> 3)] &
                    (1 << ((offset_in_bits + i) & 7))):
                result_view[i] = 0
                continue  # Null row → result[i] is 0

        outer_start = offsets[arr_offset + i]
        outer_end = offsets[arr_offset + i + 1]

        for j in range(outer_start, outer_end):
            if inner_validity is not NULL:
                if not (inner_validity[j >> 3] & (1 << (j & 7))):
                    continue  # null element

            if memcmp(data + (j * type_size), literal_ptr, type_size) != 0:
                result_view[i] = 1
                break

    return result_view


cdef uint8_t[::1] _anyop_neq_boolean_chunk(object literal, object list_array):
    """
    Compare each element in a List<Boolean> to `literal`.
    Returns a uint8 array where 1 = row contains match, 0 = no match/null.
    """
    cdef:
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
        const uint8_t* inner_values = NULL

        Py_ssize_t i, j, outer_start, outer_end, byte_index, bit_index
        uint8_t literal_val = bool(literal)

        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.ones(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

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
                result_view[i] = 0
                continue  # Null row → result[i] is 0

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
                result_view[i] = 0
                break

    return result_view


cdef uint8_t[::1] _anyop_neq_generic_chunk(object literal, object array):
    """
    Generic fallback: compare each value in the array to `literal` using Python equality.
    """
    cdef:
        Py_ssize_t row_count = len(array)
        Py_ssize_t offset = array.offset
        Py_ssize_t i
        object value
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.ones(row_count, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

    if hasattr(literal, "item"):
        literal = literal.item()
    if hasattr(array, "to_pylist"):
        array = array.to_pylist()

    for i in range(row_count):
        value = array[i + offset]
        if value is None:
            result_view[i] = 0
        elif literal in value:
            result_view[i] = 0

    return result_view

# ===== Functions from list_arrow_op.pyx =====


cpdef numpy.ndarray list_arrow_op(numpy.ndarray arr, object key):
    """
    Fetch values from a list of dictionaries based on a specified key.

    Parameters:
        data: list
            A list of dictionaries where each dictionary represents a structured record.
        key: str
            The key whose corresponding value is to be fetched from each dictionary.

    Returns:
        numpy.ndarray: An array containing the values associated with the key in each dictionary
                     or None where the key does not exist.
    """
    # Determine the number of items in the input list
    cdef Py_ssize_t n = len(arr)
    # Prepare an object array to store the results
    cdef numpy.ndarray result = numpy.empty(n, dtype=object)
    cdef dict document

    cdef Py_ssize_t i
    # Iterate over the list of dictionaries
    for i in range(n):
        # Check if the key exists in the dictionary
        document = arr[i]
        if document is not None:
            if key in document:
                result[i] = document[key]
            else:
                # Assign None if the key does not exist
                result[i] = None

    return result

# ===== Functions from list_cast_int64_to_string.pyx =====



cdef inline char* int64_to_str_ptr(int64_t value, char* buf) nogil:
    cdef uint64_t val
    cdef int i = 20
    cdef bint is_negative = value < 0

    if value == 0:
        buf[19] = 48  # '0'
        return buf + 19

    val = <uint64_t>(-value) if is_negative else <uint64_t>value

    while val != 0:
        i -= 1
        buf[i] = 48 + (val % 10)
        val //= 10

    if is_negative:
        i -= 1
        buf[i] = 45  # '-'

    return buf + i


cpdef numpy.ndarray list_cast_int64_to_bytes(const int64_t[:] arr):
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef char buf[21]
    cdef char* ptr
    cdef int length

    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object[:] result_view = result

    for i in range(n):
        ptr = int64_to_str_ptr(arr[i], buf)
        length = buf + 20 - ptr
        result_view[i] = PyBytes_FromStringAndSize(ptr, length)

    return result

cpdef numpy.ndarray list_cast_int64_to_ascii(const int64_t[:] arr):
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef char buf[21]
    cdef char* ptr
    cdef int length

    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object[:] result_view = result

    for i in range(n):
        ptr = int64_to_str_ptr(arr[i], buf)
        length = buf + 20 - ptr
        result_view[i] = PyBytes_FromStringAndSize(ptr, length).decode("ascii")

    return result

# ===== Functions from list_cast_string_to_int.pyx =====



cpdef numpy.ndarray list_cast_ascii_to_int(numpy.ndarray[object, ndim=1] arr):
    cdef Py_ssize_t i, j, n = arr.shape[0]
    cdef numpy.ndarray[int64_t] result = numpy.empty(n, dtype=numpy.int64)
    cdef int64_t[:] result_view = result

    cdef str s
    cdef int64_t value
    cdef int sign
    cdef Py_ssize_t length
    cdef char c

    for i in range(n):
        s = arr[i]
        length = len(s)
        value = 0
        sign = 1

        j = 0
        if length > 0 and s[0] == 45:  # -
            sign = -1
            j = 1

        for j in range(j, length):
            c = ord(s[j]) - 48
            if c < 0 or c > 9:
                raise ValueError(f"Invalid digit: {s[j]!r}")
            value = value * 10 + c

        result_view[i] = sign * value

    return result

cpdef numpy.ndarray list_cast_bytes_to_int(numpy.ndarray[object, ndim=1] arr):
    cdef Py_ssize_t i, j, n = arr.shape[0]
    cdef numpy.ndarray[int64_t] result = numpy.empty(n, dtype=numpy.int64)
    cdef int64_t[:] result_view = result

    cdef const char* c_str
    cdef Py_ssize_t length
    cdef int64_t value
    cdef int sign
    cdef char c

    for i in range(n):
        c_str = PyBytes_AS_STRING(arr[i])
        length = len(arr[i])
        value = 0
        sign = 1

        j = 0
        if length > 0 and c_str[0] == 45:  # -
            sign = -1
            j = 1

        for j in range(j, length):
            c = c_str[j] - 48
            if c < 0 or c > 9:
                raise ValueError(f"Invalid digit: {chr(c_str[j])!r}")
            value = value * 10 + c

        result_view[i] = sign * value

    return result

# ===== Functions from list_cast_uint64_to_string.pyx =====



cdef inline char* uint64_to_str_ptr(uint64_t value, char* buf) nogil:
    cdef int64_t val
    cdef int i = 20

    if value == 0:
        buf[19] = 48  # '0'
        return buf + 19

    val = <int64_t>value

    while val != 0:
        i -= 1
        buf[i] = 48 + (val % 10)
        val //= 10

    return buf + i

cpdef numpy.ndarray list_cast_uint64_to_bytes(const uint64_t[:] arr):
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef char buf[21]
    cdef char* ptr
    cdef int length

    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object[:] result_view = result

    for i in range(n):
        ptr = uint64_to_str_ptr(arr[i], buf)
        length = buf + 20 - ptr
        result_view[i] = PyBytes_FromStringAndSize(ptr, length)

    return result

cpdef numpy.ndarray list_cast_uint64_to_ascii(const uint64_t[:] arr):
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef char buf[21]
    cdef char* ptr
    cdef int length

    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object[:] result_view = result

    for i in range(n):
        ptr = uint64_to_str_ptr(arr[i], buf)
        length = buf + 20 - ptr
        result_view[i] = PyBytes_FromStringAndSize(ptr, length).decode("ascii")

    return result

# ===== Functions from list_contains_all.pyx =====



cpdef uint8_t[::1] list_contains_all(object[::1] array, set items):
    """
    Check if all of the elements in `items` are present in each subarray of the input array.

    Parameters:
        array: numpy.ndarray
            A numpy array of object arrays, where each subarray contains elements to be checked.
        items: set
            A Python set containing the items that must all be present.

    Returns:
        numpy.ndarray: A numpy array of uint8 (0 or 1) indicating whether all items are present
                       in the subarray (1 = all present, 0 = not all present).
    """
    cdef Py_ssize_t size = array.shape[0]
    cdef Py_ssize_t i, j
    cdef numpy.ndarray test_set
    cdef object element
    cdef set found

    cdef numpy.ndarray[numpy.uint8_t, ndim=1] res = numpy.zeros(size, dtype=numpy.uint8)
    cdef uint8_t[::1] res_view = res

    if not items:
        # If items is empty, trivially true for all rows
        res_view[:] = 1
        return res

    for i in range(size):
        test_set = array[i]
        if test_set is not None and test_set.shape[0] > 0:
            found = set()
            for j in range(test_set.shape[0]):
                element = test_set[j]
                if element in items:
                    found.add(element)
                    if len(found) == len(items):
                        res_view[i] = 1
                        break
    return res

# ===== Functions from list_contains_any.pyx =====



cpdef uint8_t[::1] list_contains_any(object[::1] array, set items):
    """
    Check if any of the elements in the subarrays of the input array are present in the items array.

    Parameters:
        array: numpy.ndarray
            A numpy array of object arrays, where each subarray contains elements to be checked.
        items: numpy.ndarray
            A numpy array containing the items to check for in the subarrays of `array`.

    Returns:
        numpy.ndarray: A numpy array of uint8 (0 or 1) indicating the presence of any items in the subarrays.
    """

    cdef Py_ssize_t size = array.shape[0]
    cdef Py_ssize_t i, j
    cdef numpy.ndarray test_set

    cdef numpy.ndarray[numpy.uint8_t, ndim=1] res = numpy.zeros(size, dtype=numpy.uint8)
    cdef uint8_t[::1] res_view = res

    for i in range(size):
        test_set = array[i]
        if test_set is not None and test_set.shape[0] > 0:
            for j in range(test_set.shape[0]):
                if test_set[j] in items:
                    res_view[i] = 1
                    break
    return res

# ===== Functions from list_encode_utf8.pyx =====



cpdef numpy.ndarray list_encode_utf8(numpy.ndarray inp):
    """
    Parallel UTF-8 encode all elements of a 1D ndarray of "object" dtype.
    """
    cdef Py_ssize_t n = inp.shape[0]
    cdef numpy.ndarray out = numpy.empty(n, dtype=object)
    cdef object[:] inp_view = inp
    cdef object[:] out_view = out

    for i in range(n):
        out_view[i] = PyUnicode_AsUTF8String(inp_view[i])

    return out

# ===== Functions from list_get_element.pyx =====


cpdef numpy.ndarray list_get_element(numpy.ndarray[object, ndim=1] array, int key):
    """
    Fetches elements from each sub-array of an array at a given index.

    Note:
        the sub array could be a numpy array a string etc.

    Parameters:
        array (numpy.ndarray): A 1D NumPy array of 1D NumPy arrays.
        key (int): The index at which to retrieve the element from each sub-array.

    Returns:
        numpy.ndarray: A NumPy array containing the elements at the given index from each sub-array.
    """
    cdef Py_ssize_t n = array.size

    # Check if the array is empty
    if n == 0:
        return numpy.empty(0, dtype=object)

    # Preallocate result array with the appropriate type
    cdef numpy.ndarray result = numpy.empty(n, dtype=object)
    cdef object[:] result_view = result
    cdef object sub_array
    cdef Py_ssize_t i = 0

    # Iterate over the array using memory views for efficient access
    for i in range(n):
        sub_array = <object>array[i]
        if sub_array is not None and len(sub_array) > key:
            result_view[i] = sub_array[key]
        else:
            result_view[i] = None

    return result

# ===== Functions from list_in_list.pyx =====


cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_in_list(object[::1] arr, set values):
    """
    Fast membership check for "InList" using Cython.

    Parameters:
        arr: NumPy array of arbitrary type (should be homogeneous).
        values: List of valid values (converted to a Cython set).

    Returns:
        NumPy boolean array indicating membership.
    """
    cdef Py_ssize_t i, size = arr.shape[0]
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(size, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result

    for i in range(size):
        result_view[i] = arr[i] in values

    return result

cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_in_list_int64(const int64_t[::1] arr, set values, Py_ssize_t size):
    """
    Fast membership check for "InList" using Cython.

    Parameters:
        arr: NumPy array of arbitrary type (should be homogeneous).
        values: List of valid values (converted to a Cython set).

    Returns:
        NumPy boolean array indicating membership.
    """
    cdef Py_ssize_t i
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(size, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result
    cdef int64_t value

    for i in range(size):
        value = arr[i]
        result_view[i] = value in values

    return result

# ===== Functions from list_in_string.pyx =====

# Define strncasecmp as _strnicmp on Windows






ctypedef int (*search_func_t)(const char*, size_t, char)
cdef search_func_t searcher


# This function sets the searcher based on the CPU architecture.
def init_searcher():
    global searcher
    cdef str arch = platform.machine().lower()
    if arch.startswith("arm") or arch.startswith("aarch64") or arch.startswith("arm64"):
        searcher = neon_search
    else:
        searcher = avx_search


# Initialize the searcher once when the module is imported.
init_searcher()


cdef inline int boyer_moore_horspool(const char *haystack, size_t haystacklen, const char *needle, size_t needlelen):
    """
    Case-sensitive Boyer-Moore-Horspool substring search.

    Parameters:
        haystack (const char *): The text to search in.
        haystacklen (size_t): The length of the haystack.
        needle (const char *): The pattern to search for.
        needlelen (size_t): The length of the needle.

    Returns:
        int: 1 if the needle exists in the haystack, 0 otherwise.
    """
    cdef unsigned char skip[256]
    cdef size_t i
    cdef size_t tail_index

    if needlelen == 0:
        return 0  # No valid search possible

    if haystacklen < needlelen:
        return 0  # Needle is longer than haystack

    # Initialize skip table
    for i in range(256):
        skip[i] = needlelen  # Default shift length

    # Populate skip table for each character in the needle
    for i in range(needlelen - 1):
        skip[<unsigned char>needle[i]] = needlelen - i - 1

    i = 0  # Reset i before main search loop

    while i <= haystacklen - needlelen:
        # Use memcmp for full substring comparison
        if memcmp(&haystack[i], needle, needlelen) == 0:
            return 1  # Match found

        # Update i based on skip table, ensuring no out-of-bounds access
        tail_index = i + needlelen - 1
        i += skip[<unsigned char>haystack[tail_index]]

    return 0  # No match found


cdef inline uint8_t[::1] _substring_in_single_array(object arrow_array, str needle):
    """
    Internal helper: performs substring search on a single Arrow array
    (StringArray or BinaryArray).
    """
    cdef:
        Py_ssize_t n = len(arrow_array)
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

        bytes needle_bytes = needle.encode('utf-8')
        const char *c_pattern = PyBytes_AsString(needle_bytes)
        size_t pattern_length = len(needle_bytes)

        # Arrow buffer pointers
        list buffers = arrow_array.buffers()
        const uint8_t* validity = NULL
        const int32_t* offsets = NULL
        const char* data = NULL

        # Arrow indexing
        size_t arr_offset = arrow_array.offset
        size_t offset_in_bits = arr_offset & 7
        size_t offset_in_bytes = arr_offset >> 3

        # For loop variables
        size_t i, byte_index, bit_index
        size_t start, end, length
        int index

    # Get raw pointers from buffers (if they exist)
    if len(buffers) > 0 and buffers[0]:
        validity = <const uint8_t*><uintptr_t>(buffers[0].address)
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*><uintptr_t>(buffers[1].address)
    if len(buffers) > 2 and buffers[2]:
        data = <const char*><uintptr_t>(buffers[2].address)

    # If needle is empty or no data rows, fill with 0
    if pattern_length == 0 or n == 0:
        for i in range(n):
            result_view[i] = 0
        return result_view

    for i in range(n):
        # Default to no-match
        result_view[i] = 0

        # Check null bit if we have a validity bitmap
        if validity is not NULL:
            byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
            bit_index = (offset_in_bits + i) & 7
            if not (validity[byte_index] & (1 << bit_index)):
                # Null → remain 0
                continue

        # Offsets for this value
        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        length = end - start
        if length < pattern_length:
            continue  # too short to contain needle

        # SIMD-based first-char check
        index = searcher(data + start, length, needle[0])
        if index == -1:
            continue

        # BMH from that index
        if boyer_moore_horspool(
            data + start + index,
            <size_t>(length - index),
            c_pattern,
            pattern_length
        ) == 1:
            result_view[i] = 1

    return result_view

cpdef uint8_t[::1] list_in_string(object column, str needle):
    """
    Search for `needle` within every row of an Arrow column (StringArray, BinaryArray,
    or ChunkedArray of those). Returns a NumPy array (dtype=uint8) with 1 for matches,
    0 otherwise (null included).

    Parameters:
        column: object
            An Arrow array or ChunkedArray of strings/binary.
        needle: str
            The pattern to find.

    Returns:
        A 1-D numpy.uint8 array of length = total rows in `column`.
        Each element is 1 if `needle` occurs in that row, else 0.
    """
    cdef:
        Py_ssize_t total_length
        numpy.ndarray[numpy.uint8_t, ndim=1] final_result
        uint8_t[::1] final_view
        Py_ssize_t offset = 0
        uint8_t[::1] chunk_view
        object chunk

    # If it's already a single array, just process and return
    if not hasattr(column, "chunks"):
        # Not a ChunkedArray
        return _substring_in_single_array(column, needle)

    # If we have a ChunkedArray, figure out total length
    total_length = 0
    for chunk in column.chunks:
        total_length += len(chunk)

    final_result = numpy.empty(total_length, dtype=numpy.uint8)
    final_view = final_result

    # Process each chunk individually, then place the results contiguously
    offset = 0
    for chunk in column.chunks:
        chunk_view = _substring_in_single_array(chunk, needle)
        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view


cdef inline int boyer_moore_horspool_case_insensitive(const char *haystack, size_t haystacklen, const char *needle, size_t needlelen):
    """
    Case-insensitive Boyer-Moore-Horspool substring search.

    Parameters:
        haystack (const char *): The text to search in.
        haystacklen (size_t): The length of the haystack.
        needle (const char *): The pattern to search for.
        needlelen (size_t): The length of the needle.

    Returns:
        int: 1 if the needle exists in the haystack, 0 otherwise.
    """
    cdef unsigned char skip[256]
    cdef size_t i, k
    cdef int j  # Use int to handle negative values safely

    if needlelen == 0:
        return -1  # No valid search possible

    if haystacklen < needlelen:
        return 0  # Needle is longer than haystack

    # Initialize skip table with default shift length
    for i in range(256):
        skip[i] = needlelen  # Default shift

    # Populate skip table with actual values from needle
    for i in range(needlelen - 1):
        skip[<unsigned char>needle[i]] = needlelen - i - 1
        skip[<unsigned char>(needle[i] ^ 32)] = needlelen - i - 1  # Case-insensitive mapping

    i = 0  # Start searching from the beginning

    while i <= haystacklen - needlelen:
        k = i + needlelen - 1
        j = needlelen - 1

        # Case-insensitive comparison of characters
        while j >= 0 and strncasecmp(&haystack[k], &needle[j], 1) == 0:
            j -= 1
            k -= 1

        if j < 0:
            return 1  # Match found

        # Move i forward based on skip table
        i += skip[<unsigned char>haystack[i + needlelen - 1]]

    return 0  # No match found


cdef inline uint8_t[::1] _substring_in_single_array_case_insensitive(object arrow_array, str needle):
    """
    Internal helper: performs case-insensitive substring search on a single
    Arrow array (StringArray or BinaryArray). No SIMD 'searcher' filter.
    """
    cdef:
        Py_ssize_t n = len(arrow_array)
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

        bytes needle_bytes = needle.encode('utf-8')
        char *c_pattern = PyBytes_AsString(needle_bytes)
        size_t pattern_length = len(needle_bytes)

        # Arrow buffer pointers
        list buffers = arrow_array.buffers()
        const uint8_t* validity = NULL
        const int32_t* offsets = NULL
        const char* data = NULL

        # Arrow indexing
        Py_ssize_t arr_offset = arrow_array.offset
        Py_ssize_t offset_in_bits = arr_offset & 7
        Py_ssize_t offset_in_bytes = arr_offset >> 3

        # Loop variables
        Py_ssize_t i, byte_index, bit_index
        Py_ssize_t start, end, length

    # Fetch raw pointers (if present)
    if len(buffers) > 0 and buffers[0]:
        validity = <const uint8_t*><uintptr_t>(buffers[0].address)
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*><uintptr_t>(buffers[1].address)
    if len(buffers) > 2 and buffers[2]:
        data = <const char*><uintptr_t>(buffers[2].address)

    # If needle is empty or array empty, everything is 0
    if pattern_length == 0 or n == 0:
        for i in range(n):
            result_view[i] = 0
        return result_view

    # Main loop
    for i in range(n):
        # Default to no match
        result_view[i] = 0

        # Check null bit
        if validity is not NULL:
            byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
            bit_index = (offset_in_bits + i) & 7
            if not (validity[byte_index] & (1 << bit_index)):
                continue  # null → 0

        # Calculate string (or binary) boundaries
        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        length = end - start

        if length < pattern_length:
            continue

        # Direct call to case-insensitive BMH
        if boyer_moore_horspool_case_insensitive(
            data + start,
            <size_t>length,
            c_pattern,
            pattern_length
        ):
            result_view[i] = 1

    return result_view


cpdef uint8_t[::1] list_in_string_case_insensitive(object column, str needle):
    """
    Perform a case-insensitive substring search on an Arrow column, which may be
    a single Array or a ChunkedArray of strings/binaries. Returns a NumPy uint8
    array (1 for match, 0 for non-match/null).

    Parameters:
        column: object
            Arrow array or ChunkedArray (StringArray/BinaryArray).
        needle: str
            Pattern to find, ignoring case.

    Returns:
        A contiguous numpy.uint8 array of length == sum(len(chunk) for chunk in column).
    """
    cdef:
        Py_ssize_t total_length = 0
        numpy.ndarray[numpy.uint8_t, ndim=1] final_result
        uint8_t[::1] final_view
        Py_ssize_t offset = 0
        uint8_t[::1] chunk_view
        object chunk

    # If it's not chunked, just do the single-array logic
    if not hasattr(column, "chunks"):
        return _substring_in_single_array_case_insensitive(column, needle)

    # Otherwise, handle chunked array
    for chunk in column.chunks:
        total_length += len(chunk)

    final_result = numpy.empty(total_length, dtype=numpy.uint8)
    final_view = final_result

    offset = 0
    for chunk in column.chunks:
        chunk_view = _substring_in_single_array_case_insensitive(chunk, needle)
        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view

# ===== Functions from list_length.pyx =====




cpdef numpy.ndarray[numpy.uint32_t, ndim=1] list_length(object array):

    cdef Py_ssize_t n
    cdef numpy.ndarray[numpy.uint32_t, ndim=1] result
    cdef uint32_t[::1] result_view
    cdef object val
    cdef uint32_t i
    cdef numpy.ndarray[numpy.int32_t, ndim=1] offsets

    # PyArrow fast path (uses offsets buffer)
    if isinstance(array, (pyarrow.Array, pyarrow.ChunkedArray, pyarrow.lib.StringArray)):
        if isinstance(array, pyarrow.ChunkedArray):
            array = array.combine_chunks()

        n = len(array)

        try:
            offsets_buffer = array.buffers()[1]
            offsets = numpy.frombuffer(offsets_buffer, dtype=numpy.int32, count=n + 1)
            return (offsets[1:] - offsets[:-1]).astype(numpy.uint32)
        except Exception:
            pass  # fallback if offsets unavailable

    n = array.shape[0]
    result = numpy.empty(n, dtype=numpy.uint32)
    result_view = result

    for i in range(n):
        val = array[i]
        if isinstance(val, (str, bytes, list, numpy.ndarray)):
            result_view[i] = len(val)
        else:
            result_view[i] = 0

    return result

# ===== Functions from list_long_arrow_op.pyx =====


cpdef numpy.ndarray list_long_arrow_op(numpy.ndarray arr, object key):
    """
    Fetch values from a list of dictionaries based on a specified key.

    Parameters:
        data: list
            A list of dictionaries where each dictionary represents a structured record.
        key: str
            The key whose corresponding value is to be fetched from each dictionary.

    Returns:
        numpy.ndarray: An array containing the values associated with the key in each dictionary
                     or None where the key does not exist.
    """
    # Determine the number of items in the input list
    cdef Py_ssize_t n = len(arr)
    # Prepare an object array to store the results
    cdef numpy.ndarray result = numpy.empty(n, dtype=object)

    cdef Py_ssize_t i
    # Iterate over the list of dictionaries
    for i in range(n):
        # Check if the key exists in the dictionary
        if key in arr[i]:
            result[i] = str(arr[i][key])
        else:
            # Assign None if the key does not exist
            result[i] = None

    return result
    return result


# Aliases for backward compatibility
cython_arrow_op = list_arrow_op
cython_long_arrow_op = list_long_arrow_op
