# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy as np
cimport numpy as cnp
from libc.stdint cimport int64_t

from opteryx.third_party.abseil.containers cimport FlatHashSet
from cpython.object cimport PyObject_Hash


cpdef tuple build_rows_indices_and_column(cnp.ndarray column_data):
    cdef int64_t row_count = column_data.shape[0]
    cdef cnp.int64_t[::1] lengths = np.empty(row_count, dtype=np.int64)
    cdef cnp.int64_t[::1] offsets = np.empty(row_count + 1, dtype=np.int64)
    cdef int64_t i
    cdef int64_t total_size = 0
    cdef cnp.dtype element_dtype = column_data[0].dtype

    if not isinstance(column_data[0], np.ndarray):
        raise TypeError("UNNEST requires an ARRAY column.")

    # Calculate lengths and total_size
    for i in range(row_count):
        lengths[i] = column_data[i].shape[0]
        total_size += lengths[i]

    # Early exit if total_size is zero
    if total_size == 0:
        return (np.array([], dtype=np.int64), np.array([], dtype=object))

    # Compute offsets for efficient slicing
    offsets[0] = 0
    for i in range(row_count):
        offsets[i + 1] = offsets[i] + lengths[i]
    cdef cnp.int64_t[::1] indices = np.empty(total_size, dtype=np.int64)
    cdef cnp.ndarray flat_data = np.empty(total_size, dtype=element_dtype)

    # Fill indices and flat_data
    for i in range(row_count):
        start = offsets[i]
        end = offsets[i + 1]
        if end > start:
            indices[start:end] = i
            flat_data[start:end] = column_data[i]

    return (indices, flat_data)


cpdef tuple build_filtered_rows_indices_and_column(cnp.ndarray column_data, set valid_values):
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
    cdef cnp.ndarray flat_data
    cdef cnp.int64_t[::1] indices
    cdef cnp.dtype element_dtype = None
    cdef object value

    # Typed sets for different data types
    cdef set valid_values_typed = None

    # Determine the dtype of the elements
    for i in range(row_count):
        array_i = column_data[i]
        if array_i is not None and array_i.size > 0:
            element_dtype = array_i.dtype
            break

    if element_dtype is None:
        element_dtype = np.object_

    # Initialize indices and flat_data arrays
    indices = np.empty(allocated_size, dtype=np.int64)
    flat_data = np.empty(allocated_size, dtype=element_dtype)

    # Handle set initialization based on element dtype
    if np.issubdtype(element_dtype, np.integer):
        valid_values_typed = set([int(v) for v in valid_values])
    elif np.issubdtype(element_dtype, np.floating):
        valid_values_typed = set([float(v) for v in valid_values])
    elif np.issubdtype(element_dtype, np.str_):
        valid_values_typed = set([unicode(v) for v in valid_values])
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
                    indices = np.resize(indices, allocated_size)
                    flat_data = np.resize(flat_data, allocated_size)
                flat_data[index] = value
                indices[index] = i
                index += 1

    if index == 0:
        return (np.array([], dtype=np.int64), np.array([], dtype=element_dtype))

    # Slice arrays to the actual used size
    indices = indices[:index]
    flat_data = flat_data[:index]

    return (indices, flat_data)


cpdef tuple list_distinct(cnp.ndarray values, cnp.int64_t[::1] indices, FlatHashSet seen_hashes=None):
    cdef:
        Py_ssize_t i, j = 0
        Py_ssize_t n = values.shape[0]
        int64_t hash_value
        int64_t[::1] new_indices = np.empty(n, dtype=np.int64)
        cnp.dtype dtype = values.dtype
        cnp.ndarray new_values = np.empty(n, dtype=dtype)

    if seen_hashes is None:
        seen_hashes = FlatHashSet()

    for i in range(n):
        v = values[i]
        hash_value = PyObject_Hash(v)
        if seen_hashes.insert(hash_value):
            new_values[j] = v
            new_indices[j] = indices[i]
            j += 1

    return new_values[:j], new_indices[:j], seen_hashes
