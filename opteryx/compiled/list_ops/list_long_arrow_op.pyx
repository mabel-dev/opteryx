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
