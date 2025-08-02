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
