# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

# Define strncasecmp as _strnicmp on Windows
cdef extern from *:
    """
    #ifdef _WIN32
    #define strncasecmp _strnicmp
    #else
    #include <ctype.h>
    #endif

    // Fast case-insensitive character comparison for ASCII
    static inline int fast_case_insensitive_eq(char a, char b) {
        if (a == b) return 1;
        // Only flip case for alphabetical characters
        if ((a >= 'a' && a <= 'z') && (b >= 'A' && b <= 'Z'))
            return (a - 32) == b;
        if ((a >= 'A' && a <= 'Z') && (b >= 'a' && b <= 'z'))
            return (a + 32) == b;
        return 0;
    }
    """

import numpy
cimport numpy
numpy.import_array()

from cpython.bytes cimport PyBytes_AsString
from libc.stdint cimport int32_t, uint8_t, uintptr_t
from libc.string cimport memchr, memcpy
import platform


cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)

cdef extern from "simd_search.h":
    int neon_search(const char *data, size_t length, char target)
    int avx_search(const char *data, size_t length, char target)
    int simd_search_substring(const char *data, size_t length, const char *pattern, size_t pattern_len)

ctypedef int (*search_func_t)(const char*, size_t, char)
cdef search_func_t searcher


# This function sets the searcher based on the CPU architecture.
def init_searcher():
    global searcher
    cdef str arch = platform.machine().lower()
    if arch.startswith("arm") or arch.startswith("aarch64") or arch.startswith("arm64"):
        searcher = neon_search
    else:
        searcher = avx_search


# Initialize the searcher once when the module is imported.
init_searcher()


cdef inline int fast_memcmp_short(const char *a, const char *b, size_t n):
    cdef uint64_t aval = 0, bval = 0
    cdef uint64_t mask

    if n == 0:
        return 0
    elif n <= 8:
        mask = ((<uint64_t>1) << (8 * n)) - 1
        memcpy(&aval, a, n)
        memcpy(&bval, b, n)
        return (aval & mask) != (bval & mask)
    else:
        return memcmp(a, b, n) != 0


cdef inline int boyer_moore_horspool(const char *haystack, size_t haystacklen,
                                     const char *needle, size_t needlelen):
    """
    Optimized case-sensitive Boyer-Moore-Horspool substring search.
    """
    cdef unsigned char skip[256]
    cdef size_t i, tail_index
    cdef unsigned char last_char

    if needlelen == 0 or haystacklen < needlelen:
        return 0

    # Fast path for single character
    if needlelen == 1:
        return memchr(haystack, needle[0], haystacklen) != NULL

    # Initialize skip table - optimized loop
    for i in range(256):
        skip[i] = needlelen

    # Populate skip table - unroll small loops
    if needlelen == 2:
        skip[<unsigned char>needle[0]] = 1
    elif needlelen == 3:
        skip[<unsigned char>needle[0]] = 2
        skip[<unsigned char>needle[1]] = 1
    else:
        for i in range(needlelen - 1):
            skip[<unsigned char>needle[i]] = needlelen - i - 1

    i = 0
    last_char = <unsigned char>needle[needlelen - 1]

    while i <= haystacklen - needlelen:
        # Check last character first for better branch prediction
        if haystack[i + needlelen - 1] == last_char:
            # Use optimized comparison for short needles
            if needlelen <= 8:
                if fast_memcmp_short(&haystack[i], needle, needlelen) == 0:
                    return 1
            else:
                if memcmp(&haystack[i], needle, needlelen) == 0:
                    return 1

        # Use precomputed skip
        tail_index = i + needlelen - 1
        i += skip[<unsigned char>haystack[tail_index]]

    return 0


cdef inline void build_bmh_skip_table(const char *needle, size_t needlelen, unsigned char *skip):
    """Optimized skip table builder"""
    cdef size_t i
    for i in range(256):
        skip[i] = needlelen

    if needlelen == 2:
        skip[<unsigned char>needle[0]] = 1
    elif needlelen == 3:
        skip[<unsigned char>needle[0]] = 2
        skip[<unsigned char>needle[1]] = 1
    else:
        for i in range(needlelen - 1):
            skip[<unsigned char>needle[i]] = needlelen - i - 1


cdef inline int boyer_moore_horspool_with_table(const char *haystack, size_t haystacklen,
                                                const char *needle, size_t needlelen,
                                                unsigned char *skip):
    """BMH with precomputed table - optimized version"""
    cdef size_t i = 0
    cdef size_t tail_index
    cdef size_t needlelen_sub1 = needlelen - 1
    cdef unsigned char last_char
    cdef unsigned char tail_char

    if needlelen == 0 or haystacklen < needlelen:
        return 0

    # Fast path for single character
    if needlelen == 1:
        return memchr(haystack, needle[0], haystacklen) != NULL

    last_char = <unsigned char>needle[needlelen_sub1]
    cdef size_t end_index = haystacklen - needlelen

    while i <= end_index:
        tail_index = i + needlelen_sub1
        tail_char = <unsigned char>haystack[tail_index]

        # Check last character first
        if tail_char == last_char:
            if haystack[i] == needle[0]:
                if needlelen <= 8:
                    if fast_memcmp_short(&haystack[i], needle, needlelen) == 0:
                        return 1
                else:
                    if memcmp(&haystack[i], needle, needlelen) == 0:
                        return 1

        i += skip[tail_char]

    return 0


cdef extern from *:
    int fast_case_insensitive_eq(char a, char b)


cdef inline int boyer_moore_horspool_case_insensitive(const char *haystack, size_t haystacklen,
                                                      const char *needle, size_t needlelen):
    """
    Optimized case-insensitive Boyer-Moore-Horspool with better ASCII handling.
    """
    cdef unsigned char skip[256]
    cdef size_t i, j, pos
    cdef char nc

    if needlelen == 0 or haystacklen < needlelen:
        return 0

    # Fast path for single character
    if needlelen == 1:
        nc = needle[0]
        for i in range(haystacklen):
            if fast_case_insensitive_eq(haystack[i], nc):
                return 1
        return 0

    # Initialize skip table
    for i in range(256):
        skip[i] = needlelen

    # Build skip table with case-insensitive consideration
    for i in range(needlelen - 1):
        nc = needle[i]
        skip[<unsigned char>nc] = needlelen - i - 1
        # Add case variants to skip table
        if nc >= 'a' and nc <= 'z':
            skip[<unsigned char>(nc - 32)] = needlelen - i - 1
        elif nc >= 'A' and nc <= 'Z':
            skip[<unsigned char>(nc + 32)] = needlelen - i - 1

    i = 0
    while i <= haystacklen - needlelen:
        pos = i + needlelen - 1

        # Check if last character matches (case-insensitive)
        if fast_case_insensitive_eq(haystack[pos], needle[needlelen - 1]):
            # Check remaining characters
            j = needlelen - 1
            while j > 0:
                if not fast_case_insensitive_eq(haystack[i + j - 1], needle[j - 1]):
                    break
                j -= 1

            if j == 0:
                return 1

        i += skip[<unsigned char>haystack[pos]]

    return 0


cdef inline uint8_t[::1] _substring_in_single_array(object arrow_array, str needle):
    """
    Optimized version with better SIMD utilization and short-circuiting.
    """
    cdef:
        Py_ssize_t n
        numpy.ndarray[numpy.uint8_t, ndim=1] result
        uint8_t[::1] result_view
        bytes needle_bytes
        const char *c_pattern
        size_t pattern_length

        # Arrow buffer pointers
        list buffers
        const uint8_t* validity
        const int32_t* offsets
        const char* data

        # Arrow indexing
        size_t arr_offset
        size_t offset_in_bits
        size_t offset_in_bytes

        # For loop variables
        size_t i, byte_index, bit_index
        size_t start, end, length
        int index

    n = len(arrow_array)
    result = numpy.zeros(n, dtype=numpy.uint8)
    result_view = result

    needle_bytes = needle.encode('utf-8')
    c_pattern = PyBytes_AsString(needle_bytes)
    pattern_length = len(needle_bytes)

    # Arrow buffer pointers
    buffers = arrow_array.buffers()
    validity = NULL
    offsets = NULL
    data = NULL

    # Arrow indexing
    arr_offset = arrow_array.offset
    offset_in_bits = arr_offset & 7
    offset_in_bytes = arr_offset >> 3

    # Early return for edge cases
    if pattern_length == 0 or n == 0:
        return result_view

    # Get raw pointers from buffers
    if len(buffers) > 0 and buffers[0]:
        validity = <const uint8_t*><uintptr_t>(buffers[0].address)
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*><uintptr_t>(buffers[1].address)
    if len(buffers) > 2 and buffers[2]:
        data = <const char*><uintptr_t>(buffers[2].address)

    # Precompute BMH skip table
    cdef unsigned char skip_table[256]
    if pattern_length > 0:
        build_bmh_skip_table(c_pattern, pattern_length, skip_table)

    # Special case: single character search
    if pattern_length == 1:
        for i in range(n):
            # Check null bit
            if validity is not NULL:
                byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
                bit_index = (offset_in_bits + i) & 7
                if not (validity[byte_index] & (1 << bit_index)):
                    continue

            start = offsets[arr_offset + i]
            end = offsets[arr_offset + i + 1]
            length = end - start

            if length > 0 and memchr(data + start, c_pattern[0], length) != NULL:
                result_view[i] = 1
        return result_view

    # Main search loop
    for i in range(n):
        # Check null bit
        if validity is not NULL:
            byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
            bit_index = (offset_in_bits + i) & 7
            if not (validity[byte_index] & (1 << bit_index)):
                continue

        # Get string boundaries
        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        length = end - start

        if length < pattern_length:
            continue

        # For very short strings, use direct search
        if length <= 16 and pattern_length <= 4:
            if boyer_moore_horspool_with_table(
                data + start, length, c_pattern, pattern_length, skip_table
            ):
                result_view[i] = 1
            continue

        # SIMD-based first-char search
        index = searcher(data + start, length, c_pattern[0])
        if index == -1:
            continue

        # BMH from SIMD-found position
        if boyer_moore_horspool_with_table(
            data + start + index,
            length - index,
            c_pattern,
            pattern_length,
            skip_table
        ):
            result_view[i] = 1

    return result_view


cpdef uint8_t[::1] list_in_string(object column, str needle):
    """
    Optimized version with better memory handling.
    """
    cdef:
        Py_ssize_t total_length
        numpy.ndarray[numpy.uint8_t, ndim=1] final_result
        uint8_t[::1] final_view
        Py_ssize_t offset = 0
        uint8_t[::1] chunk_view
        object chunk

    if not hasattr(column, "chunks"):
        return _substring_in_single_array(column, needle)

    # Precompute total length
    total_length = 0
    chunks_list = list(column.chunks)  # Convert to list once
    for chunk in chunks_list:
        total_length += len(chunk)

    final_result = numpy.zeros(total_length, dtype=numpy.uint8)
    final_view = final_result

    # Process chunks
    offset = 0
    for chunk in chunks_list:
        chunk_view = _substring_in_single_array(chunk, needle)
        final_view[offset:offset + len(chunk)] = chunk_view[:]  # Use slice assignment
        offset += len(chunk)

    return final_view


cdef inline uint8_t[::1] _substring_in_single_array_case_insensitive(object arrow_array, str needle):
    """
    Optimized case-insensitive search with better ASCII handling.
    """
    cdef:
        Py_ssize_t n
        numpy.ndarray[numpy.uint8_t, ndim=1] result
        uint8_t[::1] result_view
        bytes needle_bytes
        const char *c_pattern
        size_t pattern_length

        list buffers
        const uint8_t* validity
        const int32_t* offsets
        const char* data

        Py_ssize_t arr_offset
        Py_ssize_t offset_in_bits
        Py_ssize_t offset_in_bytes

        Py_ssize_t i, byte_index, bit_index
        Py_ssize_t start, end, length

    n = len(arrow_array)
    result = numpy.zeros(n, dtype=numpy.uint8)
    result_view = result

    needle_bytes = needle.encode('utf-8')
    c_pattern = PyBytes_AsString(needle_bytes)
    pattern_length = len(needle_bytes)

    buffers = arrow_array.buffers()
    validity = NULL
    offsets = NULL
    data = NULL

    arr_offset = arrow_array.offset
    offset_in_bits = arr_offset & 7
    offset_in_bytes = arr_offset >> 3

    if pattern_length == 0 or n == 0:
        return result_view

    if len(buffers) > 0 and buffers[0]:
        validity = <const uint8_t*><uintptr_t>(buffers[0].address)
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*><uintptr_t>(buffers[1].address)
    if len(buffers) > 2 and buffers[2]:
        data = <const char*><uintptr_t>(buffers[2].address)

    for i in range(n):
        # Check null bit
        if validity is not NULL:
            byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
            bit_index = (offset_in_bits + i) & 7
            if not (validity[byte_index] & (1 << bit_index)):
                continue

        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        length = end - start

        if length < pattern_length:
            continue

        if boyer_moore_horspool_case_insensitive(
            data + start, <size_t>length, c_pattern, pattern_length
        ):
            result_view[i] = 1

    return result_view


cpdef uint8_t[::1] list_in_string_case_insensitive(object column, str needle):
    """
    Optimized case-insensitive version.
    """
    cdef:
        Py_ssize_t total_length = 0
        numpy.ndarray[numpy.uint8_t, ndim=1] final_result
        uint8_t[::1] final_view
        Py_ssize_t offset = 0
        uint8_t[::1] chunk_view
        object chunk

    if not hasattr(column, "chunks"):
        return _substring_in_single_array_case_insensitive(column, needle)

    chunks_list = list(column.chunks)
    for chunk in chunks_list:
        total_length += len(chunk)

    final_result = numpy.empty(total_length, dtype=numpy.uint8)
    final_view = final_result

    offset = 0
    for chunk in chunks_list:
        chunk_view = _substring_in_single_array_case_insensitive(chunk, needle)
        final_view[offset:offset + len(chunk)] = chunk_view[:]
        offset += len(chunk)

    return final_view
