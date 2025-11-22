# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import platform
from libcpp.vector cimport vector

cdef extern from "simd_search.h":
    size_t neon_count(const char* data, size_t length, char target)
    size_t avx_count(const char* data, size_t length, char target)
    int neon_search(const char* data, size_t length, char target)
    int avx_search(const char* data, size_t length, char target)
    int neon_find_delimiter(const char* data, size_t length)
    int avx_find_delimiter(const char* data, size_t length)
    vector[size_t] neon_find_all(const char* data, size_t length, char target)
    vector[size_t] avx_find_all(const char* data, size_t length, char target)

cdef extern from "simd_string_ops.h":
    void simd_to_upper(char* data, size_t length)
    void simd_to_lower(char* data, size_t length)

# Architecture detection
cdef size_t (*simd_count)(const char*, size_t, char)
cdef int (*simd_search)(const char*, size_t, char)
cdef int (*simd_find_delimiter)(const char*, size_t)
cdef vector[size_t] (*simd_find_all)(const char*, size_t, char)

_arch = platform.machine().lower()
if _arch in ('arm64', 'aarch64'):
    simd_count = neon_count
    simd_search = neon_search
    simd_find_delimiter = neon_find_delimiter
    simd_find_all = neon_find_all
else:
    simd_count = avx_count
    simd_search = avx_search
    simd_find_delimiter = avx_find_delimiter
    simd_find_all = avx_find_all


# Expose simd_count to Python so higher-level code can reuse the optimized newline/count routine.
cpdef size_t count_instances(const unsigned char[::1] mv, char target=10):
    """
    Count occurrences of `target` byte in `buffer` using the SIMD-optimized implementation.

    Returns an integer count.
    """
    cdef const unsigned char* udata = &mv[0]
    cdef const char* data = <const char*>udata
    cdef size_t data_len = mv.shape[0]
    return simd_count(data, data_len, target)


# String case conversion functions
def to_upper(bytes data):
    """
    Convert ASCII characters in bytes to uppercase using SIMD.
    Non-ASCII bytes are left unchanged.

    Returns a new bytes object with uppercase characters.
    """
    cdef size_t length = len(data)
    cdef bytearray result = bytearray(data)
    cdef char* ptr = <char*> result
    simd_to_upper(ptr, length)
    return bytes(result)


def to_lower(bytes data):
    """
    Convert ASCII characters in bytes to lowercase using SIMD.
    Non-ASCII bytes are left unchanged.

    Returns a new bytes object with lowercase characters.
    """
    cdef size_t length = len(data)
    cdef bytearray result = bytearray(data)
    cdef char* ptr = <char*> result
    simd_to_lower(ptr, length)
    return bytes(result)


# Character search functions
def find_char(bytes data, int target):
    """
    Find the first occurrence of a character in bytes using SIMD.

    Returns the index of the first occurrence, or -1 if not found.
    """
    cdef const char* ptr = <const char*> (<char*> data)
    cdef size_t length = len(data)
    cdef char char_target = <char> target
    return simd_search(ptr, length, char_target)


def count_char(bytes data, int target):
    """
    Count occurrences of a character in bytes using SIMD.

    Returns the count of occurrences.
    """
    cdef const char* ptr = <const char*> (<char*> data)
    cdef size_t length = len(data)
    cdef char char_target = <char> target
    return simd_count(ptr, length, char_target)


def find_all_char(bytes data, int target):
    """
    Find all occurrences of a character in bytes using SIMD.

    Returns a list of indices where the character occurs.
    """
    cdef const char* ptr = <const char*> (<char*> data)
    cdef size_t length = len(data)
    cdef char char_target = <char> target
    cdef vector[size_t] positions = simd_find_all(ptr, length, char_target)
    return list(positions)
