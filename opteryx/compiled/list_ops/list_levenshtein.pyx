# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy as np
cimport numpy as cnp
cnp.import_array()
from libc.stdint cimport int64_t


cdef inline int64_t min3(int64_t x, int64_t y, int64_t z) nogil:
    """Utility function to find the minimum of three integers."""
    if x <= y:
        if x <= z:
            return x
        return z
    if y <= z:
        return y
    return z


cdef inline int64_t levenshtein_single(str string1, str string2):
    """
    Calculate the Levenshtein distance between two strings.

    Parameters:
        string1 (str): The first string to compare.
        string2 (str): The second string to compare.

    Returns:
        int: The Levenshtein distance between string1 and string2.
    """
    if len(string1) < len(string2):
        string1, string2 = string2, string1

    cdef int len1 = len(string1)
    cdef int len2 = len(string2) + 1

    cdef int64_t i, j

    # Allocate a numpy array and create a memory view from it
    cdef int64_t[:] dp = np.zeros((len1 + 1) * len2, dtype=np.int64)

    for i in range(len1 + 1):
        for j in range(len2):
            if i == 0:
                dp[j] = j
            elif j == 0:
                dp[i * len2] = i
            elif string1[i - 1] == string2[j - 1]:
                dp[i * len2 + j] = dp[(i - 1) * len2 + (j - 1)]
            else:
                dp[i * len2 + j] = 1 + min3(
                    dp[(i - 1) * len2 + j],  # Remove
                    dp[i * len2 + (j - 1)],  # Insert
                    dp[(i - 1) * len2 + (j - 1)]  # Replace
                )

    return dp[len1 * len2 + (len2 - 1)]


cpdef cnp.ndarray[cnp.int64_t, ndim=1] list_levenshtein(cnp.ndarray[object, ndim=1] a, cnp.ndarray[object, ndim=1] b):
    """
    Calculate Levenshtein distance for arrays of strings.

    Parameters:
        a: Array of strings
        b: Array of strings

    Returns:
        Array of integers representing Levenshtein distances
    """
    cdef Py_ssize_t size = len(a)
    cdef cnp.ndarray[cnp.int64_t, ndim=1] result = np.zeros(size, dtype=np.int64)
    cdef Py_ssize_t i

    for i in range(size):
        if a[i] is None or b[i] is None:
            result[i] = -1  # or some other value to indicate null
        else:
            result[i] = levenshtein_single(str(a[i]), str(b[i]))

    return result
