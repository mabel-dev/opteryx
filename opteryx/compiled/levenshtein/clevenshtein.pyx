# cython: language_level=3

import numpy as np  # Required for array allocation

cdef int min3(int x, int y, int z):
    """Utility function to find the minimum of three integers."""
    cdef int m = x
    if y < m:
        m = y
    if z < m:
        m = z
    return m

def levenshtein(str string1, str string2):
    """
    Calculate the Levenshtein distance between two strings.

    Parameters:
        string1 (str): The first string to compare.
        string2 (str): The second string to compare.

    Returns:
        int: The Levenshtein distance between string1 and string2.
    """
    cdef int len1 = len(string1)
    cdef int len2 = len(string2)
    cdef int i, j

    # Allocate a numpy array and create a memory view from it
    cdef int[:] dp = np.zeros((len1 + 1) * (len2 + 1), dtype=np.int32)

    for i in range(len1 + 1):
        for j in range(len2 + 1):
            if i == 0:
                dp[i * (len2 + 1) + j] = j  # First string is empty
            elif j == 0:
                dp[i * (len2 + 1) + j] = i  # Second string is empty
            elif string1[i - 1] == string2[j - 1]:
                dp[i * (len2 + 1) + j] = dp[(i - 1) * (len2 + 1) + (j - 1)]
            else:
                dp[i * (len2 + 1) + j] = 1 + min3(
                    dp[(i - 1) * (len2 + 1) + j],      # Remove
                    dp[i * (len2 + 1) + (j - 1)],      # Insert
                    dp[(i - 1) * (len2 + 1) + (j - 1)] # Replace
                )

    return dp[len1 * (len2 + 1) + len2]
