# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cython import Py_ssize_t
from libc.stdint cimport int64_t, uint8_t, int32_t

import numpy
cimport numpy
numpy.import_array()

from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.core.buffers cimport DrakenVarBuffer

cdef numpy.ndarray[numpy.uint8_t, ndim=1] list_in_list_int64_vector(Int64Vector vec, set values):
    cdef Py_ssize_t i, size = vec.ptr.length
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(size, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result
    cdef int64_t* data = <int64_t*>vec.ptr.data
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef bint null_in_values = None in values
    cdef bint is_valid

    if nulls == NULL:
        for i in range(size):
            result_view[i] = data[i] in values
    else:
        for i in range(size):
            is_valid = (nulls[i >> 3] >> (i & 7)) & 1
            if is_valid:
                result_view[i] = data[i] in values
            else:
                result_view[i] = null_in_values

    return result

cdef numpy.ndarray[numpy.uint8_t, ndim=1] list_in_list_string_vector(StringVector vec, set values):
    cdef Py_ssize_t i, size = vec.ptr.length
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(size, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result

    # Convert string values to bytes for comparison
    cdef set bytes_values = set()
    for val in values:
        if val is None:
            bytes_values.add(None)
        elif isinstance(val, bytes):
            bytes_values.add(val)
        elif isinstance(val, str):
            bytes_values.add(val.encode('utf-8'))
        else:
            bytes_values.add(val)

    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef int32_t start, end
    cdef char* data = <char*>ptr.data
    cdef uint8_t* nulls = ptr.null_bitmap
    cdef bint null_in_values = None in bytes_values
    cdef bint is_valid
    cdef bytes s

    if nulls == NULL:
        for i in range(size):
            start = ptr.offsets[i]
            end = ptr.offsets[i+1]
            s = data[start:end]
            result_view[i] = s in bytes_values
    else:
        for i in range(size):
            is_valid = (nulls[i >> 3] >> (i & 7)) & 1
            if is_valid:
                start = ptr.offsets[i]
                end = ptr.offsets[i+1]
                s = data[start:end]
                result_view[i] = s in bytes_values
            else:
                result_view[i] = null_in_values

    return result

cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_in_list(object arr, set values):
    """
    Fast membership check for "InList" using Cython.

    Parameters:
        arr: NumPy array or Draken Vector.
        values: Set of valid values.

    Returns:
        NumPy boolean array indicating membership.
    """
    # Handle Draken vectors
    if isinstance(arr, Int64Vector):
        return list_in_list_int64_vector(arr, values)
    elif isinstance(arr, StringVector):
        return list_in_list_string_vector(arr, values)
    elif isinstance(arr, Vector):
        # Fallback for other vector types - convert to pylist and use numpy path
        arr = numpy.array(arr.to_pylist(), dtype=object)

    # Handle NumPy arrays
    cdef Py_ssize_t i, size = arr.shape[0]
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(size, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result
    cdef object[::1] arr_view = arr

    for i in range(size):
        result_view[i] = arr_view[i] in values

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
