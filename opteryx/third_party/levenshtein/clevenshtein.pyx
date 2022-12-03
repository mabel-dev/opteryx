# cython: language_level=3, c_string_type=unicode, c_string_encoding=ascii

# https://towardsdatascience.com/text-similarity-w-levenshtein-distance-in-python-2f7478986e75
# https://gist.github.com/vatsal220/6aefbc245216bc9f2da8556f42e1c89c#file-lev_dist-py

from functools import lru_cache

import numpy

# Updated to Cythonizable for Opteryx

cdef extern from "string.h":
    int strlen(char *s)

def levenshtein(string1:numpy.str_, string2:numpy.str_):
    """
    This function will calculate the levenshtein distance between two input
    strings a and b

    params:
        a (String) : The first string you want to compare
        b (String) : The second string you want to compare

    returns:
        This function will return the distnace between string a and b.

    example:
        a = 'stamp'
        b = 'stomp'
        lev_dist(a,b)
        >> 1.0
    """
    @lru_cache(None)
    def min_dist(index1:int, index2:int):

        if index1 == s1len or index2 == s2len:
            return s1len - index1 + s2len - index2

        # calculate these once and reuse
        cdef int index1p1
        cdef int index2p1
        index1p1 = index1 + 1
        index2p1 = index2 + 1

        # no change required
        if string1[index1] == string2[index2]:
            return min_dist(index1p1, index2p1)

        return 1 + min(
            min_dist(index1, index2p1),  # insert character
            min_dist(index1p1, index2),  # delete character
            min_dist(index1p1, index2p1),  # replace character
        )

    cdef int s1len
    cdef int s2len

    s1len = strlen(string1)
    s2len = strlen(string2)

    return min_dist(0, 0)
