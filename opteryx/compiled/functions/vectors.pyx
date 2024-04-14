# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

import numpy as np
cimport numpy as cnp
cimport cython

from libc.stdint cimport uint32_t, int32_t, uint16_t, uint64_t
from cpython cimport PyUnicode_AsUTF8String, PyBytes_GET_SIZE

cdef double GOLDEN_RATIO_APPROX = 1.618033988749895
cdef uint32_t VECTOR_SIZE = 1024

cdef uint64_t djb2_hash(char* byte_array, uint64_t length) nogil:
    """
    Hashes a byte array using the djb2 algorithm, designed to be called without
    holding the Global Interpreter Lock (GIL).

    Parameters:
        byte_array: char*
            The byte array to hash.
        length: uint64_t
            The length of the byte array.

    Returns:
        uint64_t: The hash value.
    """
    cdef uint64_t hash_value = 5381
    cdef uint64_t i = 0
    for i in range(length):
        hash_value = ((hash_value << 5) + hash_value) + byte_array[i]
    return hash_value



def vectorize(list tokens):
    cdef cnp.ndarray[cnp.uint16_t, ndim=1] vector = np.zeros(VECTOR_SIZE, dtype=np.uint16)
    cdef uint32_t hash_1
    cdef uint32_t hash_2
    cdef bytes token_bytes
    cdef uint32_t token_size
    
    for token in tokens:
        token_bytes = PyUnicode_AsUTF8String(token)
        token_size = PyBytes_GET_SIZE(token_bytes)
        if token_size > 1:
            hash_1 = djb2_hash(token_bytes, token_size) & (VECTOR_SIZE - 1)
            hash_2 = <int32_t>(hash_1 * GOLDEN_RATIO_APPROX) & (VECTOR_SIZE - 1)
            vector[hash_1] += 1
            vector[hash_2] += 1

    return vector


def possible_match(list query_tokens, cnp.ndarray[cnp.uint16_t, ndim=1] vector):
    cdef uint32_t hash_1
    cdef uint32_t hash_2
    cdef bytes token_bytes
    cdef uint32_t token_size
    
    for token in query_tokens:
        token_bytes = PyUnicode_AsUTF8String(token)
        token_size = PyBytes_GET_SIZE(token_bytes)
        if token_size > 1:
            hash_1 = djb2_hash(token_bytes, token_size) & (VECTOR_SIZE - 1)
            hash_2 = <int32_t>(hash_1 * GOLDEN_RATIO_APPROX) & (VECTOR_SIZE - 1)
            if vector[hash_1] == 0 or vector[hash_2] == 0:
                return False  # If either position is zero, the token cannot be present
    return True