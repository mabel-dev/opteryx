# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import platform

cdef extern from "simd_search.h":
    size_t neon_count(const char* data, size_t length, char target)
    size_t avx_count(const char* data, size_t length, char target)
    int neon_search(const char* data, size_t length, char target)
    int avx_search(const char* data, size_t length, char target)
    int neon_find_delimiter(const char* data, size_t length)
    int avx_find_delimiter(const char* data, size_t length)

# Architecture detection
cdef size_t (*simd_count)(const char*, size_t, char)
cdef int (*simd_search)(const char*, size_t, char)
cdef int (*simd_find_delimiter)(const char*, size_t)

_arch = platform.machine().lower()
if _arch in ('arm64', 'aarch64'):
    simd_count = neon_count
    simd_search = neon_search
    simd_find_delimiter = neon_find_delimiter
else:
    simd_count = avx_count
    simd_search = avx_search
    simd_find_delimiter = avx_find_delimiter


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
