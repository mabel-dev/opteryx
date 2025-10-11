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


cpdef numpy.ndarray list_string_slice_left(object arr, object length):
    """
    Slice strings from the left (beginning).

    Parameters:
        arr: Array of strings
        length: Length to slice (can be scalar or array)

    Returns:
        Array of sliced strings
    """

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)

    cdef Py_ssize_t i, n = len(arr)
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef numpy.ndarray length_arr
    cdef object string_val
    cdef int slice_len

    if n == 0:
        return result

    # Handle scalar length
    if not hasattr(length, "__iter__"):
        length_arr = numpy.full(n, length, dtype=object)
    else:
        if hasattr(length, "to_numpy"):
            length_arr = length.to_numpy(zero_copy_only=False)
        else:
            length_arr = numpy.asarray(length)

    # Convert input array to numpy if needed
    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)

    for i in range(n):
        string_val = arr[i]
        if string_val is None:
            result[i] = None
        else:
            slice_len = int(length_arr[i])
            result[i] = string_val[:slice_len]

    return result


cpdef numpy.ndarray list_string_slice_right(object arr, object length):
    """
    Slice strings from the right (end).

    Parameters:
        arr: Array of strings
        length: Length to slice (can be scalar or array)

    Returns:
        Array of sliced strings
    """

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)

    cdef Py_ssize_t i, n = len(arr)
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef numpy.ndarray length_arr
    cdef object string_val
    cdef int slice_len

    if n == 0:
        return result

    # Handle scalar length
    if not hasattr(length, "__iter__"):
        length_arr = numpy.full(n, length, dtype=object)
    else:
        if hasattr(length, "to_numpy"):
            length_arr = length.to_numpy(zero_copy_only=False)
        else:
            length_arr = numpy.asarray(length)

    # Convert input array to numpy if needed
    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)

    for i in range(n):
        string_val = arr[i]
        if string_val is None:
            result[i] = None
        else:
            slice_len = int(length_arr[i])
            result[i] = string_val[-slice_len:]

    return result
