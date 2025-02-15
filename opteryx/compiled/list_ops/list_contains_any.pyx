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

from libc.stdint cimport uint8_t

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
