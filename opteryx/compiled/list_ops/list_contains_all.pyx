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
