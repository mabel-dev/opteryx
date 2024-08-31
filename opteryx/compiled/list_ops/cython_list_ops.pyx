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
from cpython.unicode cimport PyUnicode_AsUTF8String

cnp.import_array()


cpdef cnp.ndarray[cnp.npy_bool, ndim=1] cython_allop_eq(object literal, cnp.ndarray arr):
    cdef:
        Py_ssize_t i, j
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)
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
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], True, dtype=bool)
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
    cdef:
        cdef Py_ssize_t i, j
        cdef Py_ssize_t num_rows = arr.shape[0]
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(num_rows, False, dtype=bool)
        cnp.ndarray row

    for i in range(num_rows):
        row = arr[i]
        if row is not None:
            for j in range(row.shape[0]):
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
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)
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
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)
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
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)
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
        cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.full(arr.shape[0], False, dtype=bool)
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


cpdef cnp.ndarray array_encode_utf8(cnp.ndarray inp):
    """
    utf-8 encode all elements of a 1d ndarray of "object" dtype.
    A new ndarray of bytes objects is returned.

    This converts about 5 million short strings (twitter user names) per second,
    and 3 million tweets per second. Raw python is many times slower 

    Parameters:
        inp: list or ndarray
            The input array to encode.
    
    Returns:
        numpy.ndarray
            A new ndarray with utf-8 encoded bytes objects.
    """
    cdef Py_ssize_t i, n = inp.shape[0]
    cdef object[:] inp_view = inp  # Create a memory view for faster access

    # Iterate and encode
    for i in range(n):
        inp_view[i] = PyUnicode_AsUTF8String(inp_view[i])

    return inp