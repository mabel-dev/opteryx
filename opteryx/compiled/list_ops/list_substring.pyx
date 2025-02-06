# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy

from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.bytes cimport PyBytes_AsString
from libc.stdint cimport uint8_t

cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)

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


cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_substring(numpy.ndarray[numpy.str, ndim=1] haystack, str needle):
    """
    Used as the InStr operator, which was written to replace using LIKE to execute list_substring
    matching. We tried using PyArrow's substring but the performance was almost identical to LIKE.
    """
    cdef Py_ssize_t n = haystack.shape[0]
    cdef bytes needle_bytes = needle.encode('utf-8')
    cdef char *c_pattern = PyBytes_AsString(needle_bytes)
    cdef size_t pattern_length = len(needle_bytes)
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
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


cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_substring_case_insensitive(numpy.ndarray[numpy.str, ndim=1] haystack, str needle):
    """
    Used as the InStr operator, which was written to replace using LIKE to execute list_substring
    matching. We tried using PyArrow's substring but the performance was almost identical to LIKE.
    """
    cdef Py_ssize_t n = haystack.shape[0]
    cdef bytes needle_bytes = needle.encode('utf-8')
    cdef char *c_pattern = PyBytes_AsString(needle_bytes)
    cdef size_t pattern_length = len(needle_bytes)
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
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
