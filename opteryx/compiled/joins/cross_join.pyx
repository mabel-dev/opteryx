# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()

from opteryx.third_party.abseil.containers cimport FlatHashSet
from opteryx.third_party.fastfloat.fast_float cimport c_parse_fast_float as parse_fast_float

from libc.stdint cimport int32_t, int64_t, uint64_t, uint8_t, uintptr_t
from cpython.unicode cimport PyUnicode_DecodeUTF8
from cpython.object cimport PyObject_Hash
from cpython.bytes cimport PyBytes_FromStringAndSize

cpdef tuple build_rows_indices_and_column(object column):
    cdef:
        object child_elements = column.values
        list buffers = column.buffers()
        Py_ssize_t row_count = len(column)
        Py_ssize_t total_size
        Py_ssize_t i
        Py_ssize_t j
        Py_ssize_t index_pos
        numpy.ndarray indices_np, flat_data_np
        int64_t[::1] indices
        object[:] flat_data
        Py_ssize_t arr_offset = column.offset
        const int32_t* offsets32 = <const int32_t*><uintptr_t>(buffers[1].address)

        # Child array variables
        Py_ssize_t child_offset = child_elements.offset
        list child_buffers = child_elements.buffers()
        const int32_t* child_offsets32 = <const int32_t*><uintptr_t>(child_buffers[1].address)
        const char* child_data = <const char*><uintptr_t>(child_buffers[2].address)

        const uint8_t* parent_valid = <const uint8_t*><uintptr_t>(buffers[0].address) if buffers[0] else NULL
        const uint8_t* child_valid = <const uint8_t*><uintptr_t>(child_buffers[0].address) if child_buffers[0] else NULL

        Py_ssize_t start
        Py_ssize_t end
        Py_ssize_t str_start
        Py_ssize_t str_end

    if row_count == 0:
        return numpy.empty(0, dtype=numpy.int64), numpy.empty(0, dtype=object)

    total_size = offsets32[arr_offset + row_count] - offsets32[arr_offset]
    if total_size == 0:
        return numpy.empty(0, dtype=numpy.int64), numpy.empty(0, dtype=object)

    indices_np = numpy.empty(total_size, dtype=numpy.int64)
    flat_data_np = numpy.empty(total_size, dtype=object)
    indices = indices_np
    flat_data = flat_data_np

    index_pos = 0
    for i in range(row_count):
        if parent_valid and not (parent_valid[i >> 3] & (1 << (i & 7))):
            continue

        start = offsets32[arr_offset + i]
        end = offsets32[arr_offset + i + 1]

        if start >= end:
            continue

        for j in range(start, end):
            indices[index_pos] = i

            if child_valid and not (child_valid[(child_offset + j) >> 3] & (1 << ((child_offset + j) & 7))):
                flat_data[index_pos] = None
            else:
                str_start = child_offsets32[child_offset + j]
                str_end = child_offsets32[child_offset + j + 1]

                if str_end > str_start:
                    flat_data[index_pos] = PyUnicode_DecodeUTF8(
                        child_data + str_start, str_end - str_start, "replace"
                    )
                else:
                    flat_data[index_pos] = ""
            index_pos += 1

    return indices_np, flat_data_np


cpdef tuple numpy_build_rows_indices_and_column(numpy.ndarray column_data):
    cdef int64_t row_count = column_data.shape[0]
    cdef numpy.int64_t[::1] lengths = numpy.empty(row_count, dtype=numpy.int64)
    cdef numpy.int64_t[::1] offsets = numpy.empty(row_count + 1, dtype=numpy.int64)
    cdef int64_t i
    cdef int64_t total_size = 0
    cdef numpy.dtype element_dtype = column_data[0].dtype

    if not isinstance(column_data[0], numpy.ndarray):
        raise TypeError("UNNEST requires an ARRAY column.")

    # Calculate lengths and total_size
    for i in range(row_count):
        lengths[i] = column_data[i].shape[0]
        total_size += lengths[i]

    # Early exit if total_size is zero
    if total_size == 0:
        return (numpy.array([], dtype=numpy.int64), numpy.array([], dtype=object))

    # Compute offsets for efficient slicing
    offsets[0] = 0
    for i in range(row_count):
        offsets[i + 1] = offsets[i] + lengths[i]
    cdef numpy.int64_t[::1] indices = numpy.empty(total_size, dtype=numpy.int64)
    cdef numpy.ndarray flat_data = numpy.empty(total_size, dtype=element_dtype)

    # Fill indices and flat_data
    for i in range(row_count):
        start = offsets[i]
        end = offsets[i + 1]
        if end > start:
            indices[start:end] = i
            flat_data[start:end] = column_data[i]

    return (indices, flat_data)


cpdef tuple numpy_build_filtered_rows_indices_and_column(numpy.ndarray column_data, set valid_values):
    """
    Build row indices and flattened column data for matching values from a column of array-like elements.

    Parameters:
        column_data: ndarray
            An array of arrays from which to create row indices and flattened data.
        valid_values: set
            A set of values to filter the rows by during the cross join.

    Returns:
        tuple of (ndarray, ndarray)
            Returns a tuple containing an array of indices and an array of flattened data for rows that match the filter.
    """
    cdef int64_t row_count = column_data.shape[0]
    cdef int64_t allocated_size = row_count * 4  # Initial allocation size
    cdef int64_t index = 0
    cdef int64_t i, j, len_i
    cdef object array_i
    cdef numpy.ndarray flat_data
    cdef numpy.int64_t[::1] indices
    cdef numpy.dtype element_dtype = numpy.dtype(object)
    cdef object value

    # Typed sets for different data types
    cdef set valid_values_typed = None

    # Determine the dtype of the elements
    for i in range(row_count):
        array_i = column_data[i]
        if array_i is not None and array_i.size > 0:
            element_dtype = array_i.dtype
            break

    # Initialize indices and flat_data arrays
    indices = numpy.empty(allocated_size, dtype=numpy.int64)
    flat_data = numpy.empty(allocated_size, dtype=element_dtype)

    # Handle set initialization based on element dtype
    if numpy.issubdtype(element_dtype, numpy.integer):
        valid_values_typed = {int(v) for v in valid_values}
    elif numpy.issubdtype(element_dtype, numpy.floating):
        valid_values_typed = {parse_fast_float(v) for v in valid_values}
    elif numpy.issubdtype(element_dtype, numpy.str_):
        valid_values_typed = {unicode(v) for v in valid_values}
    else:
        valid_values_typed = valid_values  # Fallback to generic Python set

    # Main loop
    for i in range(row_count):
        array_i = column_data[i]
        if array_i is None:
            continue
        len_i = array_i.shape[0]
        if len_i == 0:
            continue

        for j in range(len_i):
            value = array_i[j]
            if value in valid_values_typed:
                if index >= allocated_size:
                    # Reallocate arrays
                    allocated_size *= 2
                    indices = numpy.resize(indices, allocated_size)
                    flat_data = numpy.resize(flat_data, allocated_size)
                flat_data[index] = value
                indices[index] = i
                index += 1

    if index == 0:
        return (numpy.array([], dtype=numpy.int64), numpy.array([], dtype=element_dtype))

    # Slice arrays to the actual used size
    indices = indices[:index]
    flat_data = flat_data[:index]

    return (indices, flat_data)


cpdef tuple build_filtered_rows_indices_and_column(object column, set valid_values):
    """
    Arrow-native version of build_filtered_rows_indices_and_column.
    Filters values from a ListArray column based on membership in `valid_values`.
    Returns matching row indices and values (as bytes, not str).
    """
    cdef:
        object child_elements = column.values
        list buffers = column.buffers()
        Py_ssize_t row_count = len(column)
        Py_ssize_t arr_offset = column.offset
        const int32_t* offsets32 = <const int32_t*><uintptr_t>(buffers[1].address)
        Py_ssize_t i = 0
        Py_ssize_t j = 0
        Py_ssize_t k = 0
        Py_ssize_t start
        Py_ssize_t end
        Py_ssize_t str_len
        Py_ssize_t str_end
        Py_ssize_t allocated_size = row_count * 4 if row_count > 0 else 4

        numpy.ndarray indices = numpy.empty(allocated_size, dtype=numpy.int64)
        int64_t[::1] indices_mv = indices
        numpy.ndarray flat_data = numpy.empty(allocated_size, dtype=object)
        object[::1] flat_mv = flat_data

        list child_buffers = child_elements.buffers()
        const int32_t* child_offsets32 = <const int32_t*><uintptr_t>(child_buffers[1].address)
        const char* child_data = <const char*><uintptr_t>(child_buffers[2].address)
        Py_ssize_t child_offset = child_elements.offset
        const uint8_t* parent_bitmap = NULL
        const uint8_t* child_bitmap = NULL

        Py_ssize_t str_start

    if buffers[0]:
        parent_bitmap = <const uint8_t*><uintptr_t>(buffers[0].address)
    if child_buffers[0]:
        child_bitmap = <const uint8_t*><uintptr_t>(child_buffers[0].address)

    # Normalize valid_values to bytes
    cdef set valid_bytes = set()
    for v in valid_values:
        valid_bytes.add(v.encode("utf8") if isinstance(v, str) else v)

    for i in range(row_count):
        if parent_bitmap is not NULL and not (parent_bitmap[i >> 3] & (1 << (i & 7))):
            continue

        start = offsets32[arr_offset + i]
        end = offsets32[arr_offset + i + 1]

        for j in range(start, end):
            if child_bitmap is not NULL and not (child_bitmap[(child_offset + j) >> 3] & (1 << ((child_offset + j) & 7))):
                continue

            str_start = child_offsets32[child_offset + j]
            str_end = child_offsets32[child_offset + j + 1]
            str_len = str_end - str_start

            # Materialize only matched values as bytes
            value_bytes = PyBytes_FromStringAndSize(child_data + str_start, str_len)

            if value_bytes in valid_bytes:
                if k >= allocated_size:
                    allocated_size *= 2
                    new_indices = numpy.empty(allocated_size, dtype=numpy.int64)
                    new_indices[:k] = indices_mv[:k]
                    indices = new_indices
                    indices_mv = indices

                    new_flat = numpy.empty(allocated_size, dtype=object)
                    new_flat[:k] = flat_data[:k]
                    flat_data = new_flat
                    flat_mv = flat_data

                flat_mv[k] = value_bytes
                indices_mv[k] = i
                k += 1

    return indices_mv[:k], flat_mv[:k]


cpdef tuple list_distinct(numpy.ndarray values, int64_t[::1] indices, FlatHashSet seen_hashes=None):
    cdef:
        Py_ssize_t i = 0
        Py_ssize_t j = 0
        Py_ssize_t n = values.shape[0]
        uint64_t hash_value
        object v
        numpy.dtype dtype = values.dtype
        numpy.ndarray new_values = numpy.empty(n, dtype=dtype)
        int64_t[::1] new_indices = numpy.empty(n, dtype=numpy.int64)

    if seen_hashes is None:
        seen_hashes = FlatHashSet()

    for i in range(n):
        v = values[i]
        hash_value = <uint64_t>(PyObject_Hash(v) & 0xFFFFFFFFFFFFFFFF)
        if seen_hashes.insert(hash_value):
            new_values[j] = v
            new_indices[j] = indices[i]
            j += 1

    return new_values[:j], new_indices[:j], seen_hashes
