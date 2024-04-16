# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False

import numpy as np
cimport numpy as cnp
cimport cython

from libc.stdint cimport uint32_t, uint16_t, uint64_t
from cpython cimport PyUnicode_AsUTF8String, PyBytes_GET_SIZE
from cpython.bytes cimport PyBytes_AsString

cdef double GOLDEN_RATIO_APPROX = 1.618033988749895
cdef uint32_t VECTOR_SIZE = 1024

cdef dict irregular_lemmas = {
    b'are': b'is',
    b'arose': b'arise',
    b'awoke': b'awake',
    b'was': b'be',
    b'were': b'be',
    b'born': b'bear',
    b'bore': b'bear',
    b'be': b'is',
    b'became': b'become',
    b'began': b'begin',
    b'bent': b'bend',
    b'best': b'good',
    b'better': b'good',
    b'bit': b'bite',
    b'bled': b'bleed',
    b'blew': b'blow',
    b'broke': b'break',
    b'bred': b'breed',
    b'brought': b'bring',
    b'built': b'build',
    b'burnt': b'burn',
    b'burst': b'burst',
    b'bought': b'buy',
    b'caught': b'catch',
    b'chose': b'choose',
    b'clung': b'cling',
    b'came': b'come',
    b'crept': b'creep',
    b'dealt': b'deal',
    b'dug': b'dig',
    b'did': b'do',
    b'done': b'do',
    b'drew': b'draw',
    b'drank': b'drink',
    b'drove': b'drive',
    b'ate': b'eat',
    b'famous': b'famous',
    b'fell': b'fall',
    b'fed': b'feed',
    b'felt': b'feel',
    b'fought': b'fight',
    b'found': b'find',
    b'fled': b'flee',
    b'flung': b'fling',
    b'flew': b'fly',
    b'forbade': b'forbid',
    b'forgot': b'forget',
    b'forgave': b'forgive',
    b'froze': b'freeze',
    b'got': b'get',
    b'gave': b'give',
    b'went': b'go',
    b'grew': b'grow',
    b'had': b'have',
    b'heard': b'hear',
    b'hid': b'hide',
    b'his': b'his',
    b'held': b'hold',
    b'kept': b'keep',
    b'knew': b'know',
    b'knelt': b'kneel',
    b'knew': b'know',
    b'led': b'lead',
    b'leapt': b'leap',
    b'learnt': b'learn',
    b'left': b'leave',
    b'lent': b'lend',
    b'lay': b'lie',
    b'lit': b'light',
    b'lost': b'lose',
    b'made': b'make',
    b'meant': b'mean',
    b'met': b'meet',
    b'men': b'man',
    b'paid': b'pay',
    b'people': b'person',
    b'rode': b'ride',
    b'rang': b'ring',
    b'rose': b'rise',
    b'ran': b'run',
    b'said': b'say',
    b'saw': b'see',
    b'sold': b'sell',
    b'sent': b'send',
    b'shone': b'shine',
    b'shot': b'shoot',
    b'showed': b'show',
    b'sang': b'sing',
    b'sank': b'sink',
    b'sat': b'sit',
    b'slept': b'sleep',
    b'spoke': b'speak',
    b'spent': b'spend',
    b'spun': b'spin',
    b'stood': b'stand',
    b'stole': b'steal',
    b'stuck': b'stick',
    b'strove': b'strive',
    b'sung': b'sing',
    b'swore': b'swear',
    b'swept': b'sweep',
    b'swam': b'swim',
    b'swung': b'swing',
    b'took': b'take',
    b'taught': b'teach',
    b'tore': b'tear',
    b'told': b'tell',
    b'thought': b'think',
    b'threw': b'throw',
    b'trod': b'tread',
    b'understood': b'understand',
    b'went': b'go',
    b'woke': b'wake',
    b'wore': b'wear',
    b'won': b'win',
    b'wove': b'weave',
    b'wept': b'weep',
    b'would': b'will',
    b'wrote': b'write'
}

cdef inline uint16_t djb2_hash(char* byte_array, uint64_t length) nogil:
    """
    Hashes a byte array using the djb2 algorithm, designed to be called without
    holding the Global Interpreter Lock (GIL).

    Parameters:
        byte_array: char*
            The byte array to hash.
        length: uint64_t
            The length of the byte array.

    Returns:
        uint16_t: The hash value.
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

            hash_1 = hash_1 & (VECTOR_SIZE - 1)
            if vector[hash_1] < 65535:
                vector[hash_1] += 1
            
            if vector[hash_2] < 65535:
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


def possible_match_indices(cnp.ndarray[cnp.uint16_t, ndim=1] indices, cnp.ndarray[cnp.uint16_t, ndim=1] vector):
    """
    Check if all specified indices in 'indices' have non-zero values in 'vector'.

    Parameters:
        indices: cnp.ndarray[cnp.uint16_t, ndim=1]
            Array of indices to check in the vector.
        vector: cnp.ndarray[cnp.uint16_t, ndim=1]
            Array where non-zero values are expected at the indices specified by 'indices'.

    Returns:
        bool: True if all specified indices have non-zero values, otherwise False.
    """
    cdef int i
    for i in range(indices.shape[0]):
        if vector[indices[i]] == 0:
            return False
    return True


from libc.string cimport strlen, strcpy, strtok, strchr
from libc.stdlib cimport malloc, free

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
        token = strtok(c_text, " ")
        while token != NULL:
            word = <char*>malloc(strlen(token) + 1)
            i = 0
            j = 0
            while token[i] != 0:
                if 97 <= token[i] <= 122 or 48 <= token[i] <= 57:
                    word[j] = token[i]
                    j += 1
                elif 65 <= token[i] <= 90:
                    # Convert to lowercase if it's uppercase
                    word[j] = token[i] + 32
                    j += 1
                elif token[i] == 45 and j > 0:
                    word[j] = token[i]
                    j += 1
                i += 1
            word[j] = 0
            if j > 1:
                if word in irregular_lemmas:
                    lemma = strdup(irregular_lemmas[word])
                else:
                    # Perform lemmatization
                    lemma = lemmatize(word, j)

                # Append the lemma if it's not a stop word
                if lemma not in stop_words:
                    tokens.append(lemma)

            free(word)
            token = strtok(NULL, " ")
    finally:
        free(c_text)

    return tokens


from libc.string cimport strlen, strncmp, strcpy, strcat


from libc.string cimport strlen, strncmp

cpdef inline bytes lemmatize(char* word, int word_len):

    # Check 'ing' suffix
    if word_len > 5 and strncmp(word + word_len - 3, b"ing", 3) == 0:
        if word[word_len - 4] == word[word_len - 5]:  # Double consonant
            return word[:word_len - 4]
        return word[:word_len - 3]

    # Check 'ed' suffix
    if word_len > 4 and strncmp(word + word_len - 2, b"ed", 2) == 0:
        if word[word_len - 3] == word[word_len - 4]:
            return word[:word_len - 3]
        return word[:word_len - 2]

    # Check 'ly' suffix
    if word_len > 5 and strncmp(word + word_len - 2, b"ly", 2) == 0:
        if word[word_len - 3] == word[word_len - 4]:
            return word[:word_len - 3]
        return word[:word_len - 2]

    # Check 'ation' suffix
    if word_len > 8 and strncmp(word + word_len - 5, b"ation", 5) == 0:
        return word[:word_len - 5] + b'e'

    # Check 'ment' suffix
    if word_len > 8 and strncmp(word + word_len - 4, b"ation", 4) == 0:
        return word[:word_len - 4]

    # Check 's' suffix
    if word_len > 2 and strncmp(word + word_len - 1, b"s", 1) == 0:
        return word[:word_len - 1]

    return word  # Return the original if no suffix matches

