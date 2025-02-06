# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy

cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_contains_any(numpy.ndarray array, numpy.ndarray items):
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

    cdef set items_set = set(items[0])
    cdef Py_ssize_t size = array.shape[0]
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] res = numpy.zeros(size, dtype=numpy.uint8)
    cdef Py_ssize_t i
    cdef numpy.ndarray test_set

    for i in range(size):
        test_set = array[i]
        if not(test_set is None or test_set.shape[0] == 0):
            for el in test_set:
                if el in items_set:
                    res[i] = 1
                    break
    return res
