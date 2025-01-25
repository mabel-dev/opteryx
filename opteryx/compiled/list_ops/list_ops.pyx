# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

import numpy
cimport numpy as cnp
from cython import Py_ssize_t
from libc.stdint cimport int64_t, uint8_t
from numpy cimport ndarray
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.bytes cimport PyBytes_AsString
from cpython.object cimport PyObject_Hash

from opteryx.third_party.abseil.containers cimport FlatHashSet

cnp.import_array()

cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)

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


cdef inline int boyer_moore_horspool(const char *haystack, size_t haystacklen, const char *needle, size_t needlelen):
    """
    Case-sensitive Boyer-Moore-Horspool substring search.

    Parameters:
        haystack (const char *): The text to search in.
        haystacklen (size_t): The length of the haystack.
        needle (const char *): The pattern to search for.
        needlelen (size_t): The length of the needle.

    Returns:
        int: 1 if the needle exists in the haystack, 0 otherwise.
    """
    cdef unsigned char skip[256]
    cdef size_t i

    if needlelen == 0:
        return -1  # No valid search possible

    if haystacklen < needlelen:
        return 0  # Needle is longer than haystack

    # Initialize skip table
    for i in range(256):
        skip[i] = needlelen  # Default shift length

    # Populate skip table for each character in the needle
    for i in range(needlelen - 1):
        skip[<unsigned char>needle[i]] = needlelen - i - 1

    i = 0  # Reset i before main search loop

    while i <= haystacklen - needlelen:
        # Use memcmp for full substring comparison
        if memcmp(&haystack[i], needle, needlelen) == 0:
            return 1  # Match found

        # Update i based on skip table, ensuring no out-of-bounds access
        i += skip[<unsigned char>haystack[min(i + needlelen - 1, haystacklen - 1)]]

    return 0  # No match found


cdef inline int boyer_moore_horspool_case_insensitive(const char *haystack, size_t haystacklen, const char *needle, size_t needlelen):
    """
    Case-insensitive Boyer-Moore-Horspool substring search.

    Parameters:
        haystack (const char *): The text to search in.
        haystacklen (size_t): The length of the haystack.
        needle (const char *): The pattern to search for.
        needlelen (size_t): The length of the needle.

    Returns:
        int: 1 if the needle exists in the haystack, 0 otherwise.
    """
    cdef unsigned char skip[256]
    cdef size_t i, k
    cdef int j  # Use int to handle negative values safely

    if needlelen == 0:
        return -1  # No valid search possible

    if haystacklen < needlelen:
        return 0  # Needle is longer than haystack

    # Initialize skip table with default shift length
    for i in range(256):
        skip[i] = needlelen  # Default shift

    # Populate skip table with actual values from needle
    for i in range(needlelen - 1):
        skip[<unsigned char>needle[i]] = needlelen - i - 1
        skip[<unsigned char>(needle[i] ^ 32)] = needlelen - i - 1  # Case-insensitive mapping

    i = 0  # Start searching from the beginning

    while i <= haystacklen - needlelen:
        k = i + needlelen - 1
        j = needlelen - 1

        # Case-insensitive comparison of characters
        while j >= 0 and strncasecmp(&haystack[k], &needle[j], 1) == 0:
            j -= 1
            k -= 1

        if j < 0:
            return 1  # Match found

        # Move i forward based on skip table
        i += skip[<unsigned char>haystack[i + needlelen - 1]]

    return 0  # No match found


cpdef cnp.ndarray[cnp.uint8_t, ndim=1] list_substring(cnp.ndarray[cnp.str, ndim=1] haystack, str needle):
    """
    Used as the InStr operator, which was written to replace using LIKE to execute list_substring
    matching. We tried using PyArrow's substring but the performance was almost identical to LIKE.
    """
    cdef Py_ssize_t n = haystack.shape[0]
    cdef bytes needle_bytes = needle.encode('utf-8')
    cdef char *c_pattern = PyBytes_AsString(needle_bytes)
    cdef size_t pattern_length = len(needle_bytes)
    cdef cnp.ndarray[cnp.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
    cdef Py_ssize_t i = 0
    cdef Py_ssize_t length
    cdef char *data

    cdef uint8_t[::1] result_view = result

    # Check the type of the first item to decide the processing method
    if isinstance(haystack[0], str):
        for i in range(n):
            item = PyUnicode_AsUTF8String(haystack[i])
            data = <char*> PyBytes_AsString(item)
            length = len(item)
            result_view[i] = 0
            if length >= pattern_length:
                if boyer_moore_horspool(data, length, c_pattern, pattern_length):
                    result_view[i] = 1
    else:
        for i in range(n):
            item = haystack[i]
            data = <char*> item
            length = len(item)
            result_view[i] = 0
            if length >= pattern_length:
                if boyer_moore_horspool(data, length, c_pattern, pattern_length):
                    result_view[i] = 1

    return result


cpdef cnp.ndarray[cnp.uint8_t, ndim=1] list_substring_case_insensitive(cnp.ndarray[cnp.str, ndim=1] haystack, str needle):
    """
    Used as the InStr operator, which was written to replace using LIKE to execute list_substring
    matching. We tried using PyArrow's substring but the performance was almost identical to LIKE.
    """
    cdef Py_ssize_t n = haystack.shape[0]
    cdef bytes needle_bytes = needle.encode('utf-8')
    cdef char *c_pattern = PyBytes_AsString(needle_bytes)
    cdef size_t pattern_length = len(needle_bytes)
    cdef cnp.ndarray[cnp.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
    cdef Py_ssize_t i = 0
    cdef Py_ssize_t length
    cdef char *data

    cdef uint8_t[::1] result_view = result

    # Check the type of the first item to decide the processing method
    if isinstance(haystack[0], str):
        for i in range(n):
            item = PyUnicode_AsUTF8String(haystack[i])
            data = <char*> PyBytes_AsString(item)
            length = len(item)
            result_view[i] = 0
            if length >= pattern_length:
                if boyer_moore_horspool_case_insensitive(data, length, c_pattern, pattern_length):
                    result_view[i] = 1
    else:
        for i in range(n):
            item = haystack[i]
            data = <char*> item
            length = len(item)
            result_view[i] = 0
            if length >= pattern_length:
                if boyer_moore_horspool_case_insensitive(data, length, c_pattern, pattern_length):
                    result_view[i] = 1

    return result

cpdef FlatHashSet count_distinct(cnp.ndarray[cnp.int64_t, ndim=1] values, FlatHashSet seen_hashes=None):
    cdef:
        int64_t i
        int64_t n = values.shape[0]
        int64_t *values_ptr = &values[0]  # Direct pointer access

    for i in range(n):
        seen_hashes.just_insert(values_ptr[i])

    return seen_hashes

cpdef cnp.ndarray[cnp.int64_t, ndim=1] hash_column(cnp.ndarray values):

    cdef:
        int64_t i
        int64_t n = values.shape[0]
        int64_t hash_value
        #object[:] values_view = values
        cnp.ndarray[cnp.int64_t, ndim=1] result = numpy.empty(n, dtype=numpy.int64)

    for i in range(n):
        hash_value = PyObject_Hash(values[i])
        result[i] = hash_value

    return result

cpdef cnp.ndarray[cnp.int64_t, ndim=1] hash_bytes_column(cnp.ndarray[cnp.bytes] values):
    """
    Computes hash for each byte sequence in an array.

    xxHash and Murmur had too many clashes to be useful.

    Parameters:
        values (ndarray): NumPy array of bytes objects

    Returns:
        ndarray: NumPy array of int64 hashes
    """
    cdef:
        Py_ssize_t i, n = values.shape[0]
        int64_t[::1] result_view = numpy.empty(n, dtype=numpy.int64)
        bytes[::1] values_view = values

    for i in range(n):
        result_view[i] = PyObject_Hash(values_view[i])

    return numpy.asarray(result_view, dtype=numpy.int64)
