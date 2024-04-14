# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

import numpy as np
cimport numpy as cnp
cimport cython

from libc.stdint cimport uint32_t, int32_t, uint16_t, uint64_t
from cpython cimport PyUnicode_AsUTF8String, PyBytes_GET_SIZE
from cpython.bytes cimport PyBytes_AsString

cdef double GOLDEN_RATIO_APPROX = 1.618033988749895
cdef uint32_t VECTOR_SIZE = 1024

cdef uint16_t djb2_hash(char* byte_array, uint64_t length) nogil:
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
    cdef uint32_t hash_value = 5381
    cdef uint32_t i = 0
    for i in range(length):
        hash_value = ((hash_value << 5) + hash_value) + byte_array[i]
    return <uint16_t>(hash_value & 0xFFFF)




def vectorize(list tokens):
    cdef cnp.ndarray[cnp.uint16_t, ndim=1] vector = np.zeros(VECTOR_SIZE, dtype=np.uint16)
    cdef uint32_t hash_1
    cdef uint32_t hash_2
    cdef bytes token_bytes
    cdef uint32_t token_size
    
    for token_bytes in tokens:
        token_size = PyBytes_GET_SIZE(token_bytes)
        if token_size > 1:
            hash_1 = djb2_hash(token_bytes, token_size)
            hash_2 = <uint16_t>((hash_1 * GOLDEN_RATIO_APPROX)) & (VECTOR_SIZE - 1)
            vector[hash_1 & (VECTOR_SIZE - 1)] += 1
            vector[hash_2] += 1

    return vector


def possible_match(list query_tokens, cnp.ndarray[cnp.uint16_t, ndim=1] vector):
    cdef uint16_t hash_1
    cdef uint16_t hash_2
    cdef bytes token_bytes
    cdef uint32_t token_size
    
    for token_bytes in query_tokens:
        token_size = PyBytes_GET_SIZE(token_bytes)
        if token_size > 1:
            hash_1 = djb2_hash(token_bytes, token_size)
            hash_2 = <uint16_t>((hash_1 * GOLDEN_RATIO_APPROX)) & (VECTOR_SIZE - 1)
            if vector[hash_1 & (VECTOR_SIZE - 1)] == 0 or vector[hash_2] == 0:
                return False  # If either position is zero, the token cannot be present
    return True



from libc.string cimport strlen, strcpy, strtok, strchr
from libc.stdlib cimport malloc, free
import numpy as np
cimport numpy as cnp

cdef char* strdup(const char* s) nogil:
    cdef char* d = <char*>malloc(strlen(s) + 1)
    if d:
        strcpy(d, s)
    return d

cpdef list tokenize_and_remove_punctuation(str text, set stop_words):
    cdef:
        char* token
        char* word
        char* c_text
        bytes py_text = PyUnicode_AsUTF8String(text)
        list tokens = []
        int i
        int j

    # Duplicate the C string because strtok modifies the input string
    c_text = strdup(PyBytes_AsString(py_text))

    try:
        token = strtok(c_text, " ,.!?\n\t")
        while token != NULL:
            word = <char*>malloc(strlen(token) + 1)
            i = 0
            j = 0
            while token[i] != b'\0':
                # Check if the character is a lowercase or uppercase letter
                if (b'a' <= token[i] <= b'z' or b'A' <= token[i] <= b'Z'):
                    # Convert to lowercase if it's uppercase
                    word[j] = token[i] + 32 if b'A' <= token[i] <= b'Z' else token[i]
                    j += 1
                i += 1
            word[j] = b'\0'
            # Ensure the token is longer than one character and not a stop word
            if strlen(word) > 1 and word.decode('utf-8') not in stop_words:
                tokens.append(word)
            free(word)
            token = strtok(NULL, " ,.!?\n\t")
    finally:
        free(c_text)

    return tokens
