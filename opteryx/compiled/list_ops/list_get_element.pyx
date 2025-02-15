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
    Fetches elements from each sub-array of a NumPy array at a given index.

    Parameters:
        array (numpy.ndarray): A 1D NumPy array of 1D NumPy arrays.
        key (int): The index at which to retrieve the element from each sub-array.

    Returns:
        numpy.ndarray: A NumPy array containing the elements at the given index from each sub-array.
    """

    # Check if the array is empty
    if array.size == 0:
        return numpy.array([])

    # Preallocate result array with the appropriate type
    cdef numpy.ndarray result = numpy.empty(array.size, dtype=object)

    # Iterate over the array using memory views for efficient access
    cdef Py_ssize_t i = 0
    for sub_array in array:
        if sub_array is not None and len(sub_array) > key:
            result[i] = sub_array[key]
        else:
            result[i] = None
        i += 1

    return result
