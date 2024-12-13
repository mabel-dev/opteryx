# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

import cython
import numpy
cimport numpy as cnp
from cython import Py_ssize_t
from numpy cimport int64_t, ndarray
from cpython.unicode cimport PyUnicode_AsUTF8String

cnp.import_array()


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_allop_eq(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.zeros(arr.shape[0], dtype=bool)
        cnp.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]

        if row is None: 
            result[i] = False
            break

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
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.ones(arr.shape[0], dtype=bool)
        cnp.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is not None:
            for j in range(row.shape[0]):
                if row[j] == literal:
                    result[i] = False
                    break
        else:
            result[i] = False

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_eq(object literal, cnp.ndarray arr):
    """
    Check each row in arr for the presence of `literal`. If found, mark the corresponding
    position in the result as True, otherwise False.

    Parameters:
        literal: object
            The value to search for in each row.
        arr: cnp.ndarray
            A two-dimensional array-like structure where each element is a sub-array (row).

    Returns:
        cnp.ndarray[cnp.npy_bool, ndim=1]
            A boolean array indicating for each row whether `literal` was found.
    """
    cdef Py_ssize_t i, j, num_rows, row_length
    num_rows = arr.shape[0]

    cdef cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.zeros(num_rows, dtype=bool)
    cdef cnp.ndarray row

    for i in range(num_rows):
        row = arr[i]
        if row is not None:
            row_length = row.shape[0]
            if row_length > 0:
                for j in range(row_length):
                    if row[j] == literal:
                        result[i] = True
                        break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_neq(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.ones(arr.shape[0], dtype=bool)  # Default to True
        cnp.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is None:
            result[i] = False
            continue

        if row.size == 0:
            continue  # Keep result[i] as True for empty rows

        for j in range(row.size):
            if row[j] == literal:
                result[i] = False  # Found a match, set to False
                break  # No need to check the rest of the elements in this row

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_gt(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.zeros(arr.shape[0], dtype=bool)
        cnp.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is not None:
            for j in range(row.shape[0]):
                if row[j] > literal:
                    result[i] = True
                    break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_lt(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.zeros(arr.shape[0], dtype=bool)
        cnp.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is not None:
            for j in range(row.shape[0]):
                if row[j] < literal:
                    result[i] = True
                    break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_lte(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.zeros(arr.shape[0], dtype=bool)
        cnp.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is not None:
            for j in range(row.shape[0]):
                if row[j] <= literal:
                    result[i] = True
                    break

    return result


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_anyop_gte(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.zeros(arr.shape[0], dtype=bool)
        cnp.ndarray row

    for i in range(arr.shape[0]):
        row = arr[i]
        if row is not None:
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
    cdef Py_ssize_t n = len(arr)
    # Prepare an object array to store the results
    cdef cnp.ndarray result = numpy.empty(n, dtype=object)
    cdef dict document
    
    cdef Py_ssize_t i
    # Iterate over the list of dictionaries
    for i in range(n):
        # Check if the key exists in the dictionary
        document = arr[i]
        if document is not None:
            if key in document:
                result[i] = document[key]
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
    cdef Py_ssize_t n = len(arr)
    # Prepare an object array to store the results
    cdef cnp.ndarray result = numpy.empty(n, dtype=object)
    
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


cpdef cnp.ndarray cython_get_element_op(cnp.ndarray[object, ndim=1] array, int key):
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
    cdef Py_ssize_t i = 0
    for sub_array in array:
        if sub_array is not None and len(sub_array) > key:
            result[i] = sub_array[key]
        else:
            result[i] = None
        i += 1

    return result


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef cnp.ndarray array_encode_utf8(cnp.ndarray inp):
    """
    Parallel UTF-8 encode all elements of a 1D ndarray of "object" dtype.
    """
    cdef Py_ssize_t n = inp.shape[0]
    cdef cnp.ndarray out = numpy.empty(n, dtype=object)
    cdef object[:] inp_view = inp
    cdef object[:] out_view = out

    for i in range(n):
        out_view[i] = PyUnicode_AsUTF8String(inp_view[i])

    return out


cpdef cnp.ndarray[cnp.uint8_t, ndim=1] list_contains_any(cnp.ndarray array, cnp.ndarray items):
    """
    Cython optimized version that works with object arrays.
    """
    cdef set items_set = set(items[0])
    cdef Py_ssize_t size = array.shape[0]
    cdef cnp.ndarray[cnp.uint8_t, ndim=1] res = numpy.zeros(size, dtype=numpy.uint8)
    cdef Py_ssize_t i
    cdef cnp.ndarray test_set

    for i in range(size):
        test_set = array[i]
        if not(test_set is None or test_set.shape[0] == 0):
            for el in test_set:
                if el in items_set:
                    res[i] = 1
                    break
    return res
