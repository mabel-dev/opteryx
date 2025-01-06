# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy as np  # Required for array allocation
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

cpdef int64_t levenshtein(str string1, str string2):
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
