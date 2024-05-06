# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

import cython
import numpy
cimport numpy as cnp
from cython import Py_ssize_t
from cython.parallel import prange
from numpy cimport int64_t, ndarray

cnp.import_array()


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_allop_eq(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]
        if len(row) == 0:
            result[i] = False
            continue

        for j in range(row.shape[0]):
            if row[j] != literal:
                result[i] = False
                break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_allop_neq(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], True, dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]

        for j in range(row.shape[0]):
            if row[j] == literal:
                result[i] = False
                break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_eq(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]
        for j in range(row.shape[0]):
            if row[j] == literal:
                result[i] = True
                break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_neq(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.ones(arr.shape[0], dtype=bool)  # Default to True

    for i in range(arr.shape[0]):
        row = arr[i]
        if row.size == 0:
            continue  # Keep result[i] as True for empty rows

        for j in range(row.shape[0]):
            if row[j] == literal:
                result[i] = False  # Found a match, set to False
                break  # No need to check the rest of the elements in this row

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_gt(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]
        for j in range(row.shape[0]):
            if row[j] > literal:
                result[i] = True
                break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_lt(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]
        for j in range(row.shape[0]):
            if row[j] < literal:
                result[i] = True
                break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_lte(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]
        for j in range(row.shape[0]):
            if row[j] <= literal:
                result[i] = True
                break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_gte(object literal, cnp.ndarray arr):
    cdef:
        int i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)

    for i in range(arr.shape[0]):
        row = arr[i]
        for j in range(row.shape[0]):
            if row[j] >= literal:
                result[i] = True
                break

    return result


cpdef cnp.ndarray cython_arrow_op(cnp.ndarray arr, object key):
    """
    Fetch values from a list of dictionaries based on a specified key.

    Parameters:
        data: list
            A list of dictionaries where each dictionary represents a structured record.
        key: str
            The key whose corresponding value is to be fetched from each dictionary.

    Returns:
        cnp.ndarray: An array containing the values associated with the key in each dictionary
                     or None where the key does not exist.
    """
    # Determine the number of items in the input list
    cdef int n = len(arr)
    # Prepare an object array to store the results
    cdef cnp.ndarray result = numpy.empty(n, dtype=object)
    
    cdef int i
    # Iterate over the list of dictionaries
    for i in range(n):
        # Check if the key exists in the dictionary
        if key in arr[i]:
            result[i] = arr[i][key]
        else:
            # Assign None if the key does not exist
            result[i] = None

    return result


cpdef cnp.ndarray cython_long_arrow_op(cnp.ndarray arr, object key):
    """
    Fetch values from a list of dictionaries based on a specified key.

    Parameters:
        data: list
            A list of dictionaries where each dictionary represents a structured record.
        key: str
            The key whose corresponding value is to be fetched from each dictionary.

    Returns:
        cnp.ndarray: An array containing the values associated with the key in each dictionary
                     or None where the key does not exist.
    """
    # Determine the number of items in the input list
    cdef int n = len(arr)
    # Prepare an object array to store the results
    cdef cnp.ndarray result = numpy.empty(n, dtype=object)
    
    cdef int i
    # Iterate over the list of dictionaries
    for i in range(n):
        # Check if the key exists in the dictionary
        if key in arr[i]:
            result[i] = str(arr[i][key])
        else:
            # Assign None if the key does not exist
            result[i] = None

    return result


cpdef cython_get_element_op(cnp.ndarray[object, ndim=1] array, int key):
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
    cdef cnp.ndarray result = numpy.empty(array.size, dtype=object)

    # Iterate over the array using memory views for efficient access
    cdef int i = 0
    for sub_array in array:
        if sub_array is not None and len(sub_array) > key:
            result[i] = sub_array[key]
        else:
            result[i] = None
        i += 1

    return result