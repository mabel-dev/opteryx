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
    #endif
    """

import numpy
cimport numpy
numpy.import_array()

from cpython.bytes cimport PyBytes_AsString
from libc.stdint cimport int32_t, uint8_t, uintptr_t
import platform


cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)

cdef extern from "simd_search.h":
    int neon_search(const char *data, size_t length, char target)
    int avx_search(const char *data, size_t length, char target)

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


cdef inline int boyer_moore_horspool(const char *haystack, size_t haystacklen, const char *needle, size_t needlelen):
    """
    Case-sensitive Boyer-Moore-Horspool substring search.

    Parameters:
        haystack (const char *): The text to search in.
        haystacklen (size_t): The length of the haystack.
        needle (const char *): The pattern to search for.
        needlelen (size_t): The length of the needle.

    Returns:
        int: 1 if the needle exists in the haystack, 0 otherwise.
    """
    cdef unsigned char skip[256]
    cdef size_t i

    if needlelen == 0:
        return -1  # No valid search possible

    if haystacklen < needlelen:
        return 0  # Needle is longer than haystack

    # Initialize skip table
    for i in range(256):
        skip[i] = needlelen  # Default shift length

    # Populate skip table for each character in the needle
    for i in range(needlelen - 1):
        skip[<unsigned char>needle[i]] = needlelen - i - 1

    i = 0  # Reset i before main search loop

    while i <= haystacklen - needlelen:
        # Use memcmp for full substring comparison
        if memcmp(&haystack[i], needle, needlelen) == 0:
            return 1  # Match found

        # Update i based on skip table, ensuring no out-of-bounds access
        i += skip[<unsigned char>haystack[min(i + needlelen - 1, haystacklen - 1)]]

    return 0  # No match found


cdef inline uint8_t[::1] _substring_in_single_array(object arrow_array, str needle):
    """
    Internal helper: performs substring search on a single Arrow array
    (StringArray or BinaryArray).
    """
    cdef:
        Py_ssize_t n = len(arrow_array)
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

        bytes needle_bytes = needle.encode('utf-8')
        char *c_pattern = PyBytes_AsString(needle_bytes)
        size_t pattern_length = len(needle_bytes)

        # Arrow buffer pointers
        list buffers = arrow_array.buffers()
        const uint8_t* validity = NULL
        const int32_t* offsets = NULL
        const char* data = NULL

        # Arrow indexing
        Py_ssize_t arr_offset = arrow_array.offset
        Py_ssize_t offset_in_bits = arr_offset & 7
        Py_ssize_t offset_in_bytes = arr_offset >> 3

        # For loop variables
        Py_ssize_t i, byte_index, bit_index
        Py_ssize_t start, end, length
        int index

    # Get raw pointers from buffers (if they exist)
    if len(buffers) > 0 and buffers[0]:
        validity = <const uint8_t*><uintptr_t>(buffers[0].address)
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*><uintptr_t>(buffers[1].address)
    if len(buffers) > 2 and buffers[2]:
        data = <const char*><uintptr_t>(buffers[2].address)

    # If needle is empty or no data rows, fill with 0
    if pattern_length == 0 or n == 0:
        for i in range(n):
            result_view[i] = 0
        return result_view

    for i in range(n):
        # Default to no-match
        result_view[i] = 0

        # Check null bit if we have a validity bitmap
        if validity is not NULL:
            byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
            bit_index = (offset_in_bits + i) & 7
            if not (validity[byte_index] & (1 << bit_index)):
                # Null → remain 0
                continue

        # Offsets for this value
        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        length = end - start
        if length < pattern_length:
            continue  # too short to contain needle

        # SIMD-based first-char check
        index = searcher(data + start, length, needle[0])
        if index == -1:
            continue

        # BMH from that index
        if boyer_moore_horspool(
            data + start + index,
            <size_t>(length - index),
            c_pattern,
            pattern_length
        ):
            result_view[i] = 1

    return result_view

cpdef uint8_t[::1] list_substring(object column, str needle):
    """
    Search for `needle` within every row of an Arrow column (StringArray, BinaryArray,
    or ChunkedArray of those). Returns a NumPy array (dtype=uint8) with 1 for matches,
    0 otherwise (null included).

    Parameters:
        column: object
            An Arrow array or ChunkedArray of strings/binary.
        needle: str
            The pattern to find.

    Returns:
        A 1-D numpy.uint8 array of length = total rows in `column`.
        Each element is 1 if `needle` occurs in that row, else 0.
    """
    cdef:
        Py_ssize_t total_length
        numpy.ndarray[numpy.uint8_t, ndim=1] final_result
        uint8_t[::1] final_view
        Py_ssize_t offset = 0
        uint8_t[::1] chunk_view
        object chunk

    # If it's already a single array, just process and return
    if not hasattr(column, "chunks"):
        # Not a ChunkedArray
        return _substring_in_single_array(column, needle)

    # If we have a ChunkedArray, figure out total length
    total_length = 0
    for chunk in column.chunks:
        total_length += len(chunk)

    final_result = numpy.empty(total_length, dtype=numpy.uint8)
    final_view = final_result

    # Process each chunk individually, then place the results contiguously
    offset = 0
    for chunk in column.chunks:
        chunk_view = _substring_in_single_array(chunk, needle)
        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view


cdef inline int boyer_moore_horspool_case_insensitive(const char *haystack, size_t haystacklen, const char *needle, size_t needlelen):
    """
    Case-insensitive Boyer-Moore-Horspool substring search.

    Parameters:
        haystack (const char *): The text to search in.
        haystacklen (size_t): The length of the haystack.
        needle (const char *): The pattern to search for.
        needlelen (size_t): The length of the needle.

    Returns:
        int: 1 if the needle exists in the haystack, 0 otherwise.
    """
    cdef unsigned char skip[256]
    cdef size_t i, k
    cdef int j  # Use int to handle negative values safely

    if needlelen == 0:
        return -1  # No valid search possible

    if haystacklen < needlelen:
        return 0  # Needle is longer than haystack

    # Initialize skip table with default shift length
    for i in range(256):
        skip[i] = needlelen  # Default shift

    # Populate skip table with actual values from needle
    for i in range(needlelen - 1):
        skip[<unsigned char>needle[i]] = needlelen - i - 1
        skip[<unsigned char>(needle[i] ^ 32)] = needlelen - i - 1  # Case-insensitive mapping

    i = 0  # Start searching from the beginning

    while i <= haystacklen - needlelen:
        k = i + needlelen - 1
        j = needlelen - 1

        # Case-insensitive comparison of characters
        while j >= 0 and strncasecmp(&haystack[k], &needle[j], 1) == 0:
            j -= 1
            k -= 1

        if j < 0:
            return 1  # Match found

        # Move i forward based on skip table
        i += skip[<unsigned char>haystack[i + needlelen - 1]]

    return 0  # No match found


cdef inline uint8_t[::1] _substring_in_single_array_case_insensitive(object arrow_array, str needle):
    """
    Internal helper: performs case-insensitive substring search on a single
    Arrow array (StringArray or BinaryArray). No SIMD 'searcher' filter.
    """
    cdef:
        Py_ssize_t n = len(arrow_array)
        numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint8)
        uint8_t[::1] result_view = result

        bytes needle_bytes = needle.encode('utf-8')
        char *c_pattern = PyBytes_AsString(needle_bytes)
        size_t pattern_length = len(needle_bytes)

        # Arrow buffer pointers
        list buffers = arrow_array.buffers()
        const uint8_t* validity = NULL
        const int32_t* offsets = NULL
        const char* data = NULL

        # Arrow indexing
        Py_ssize_t arr_offset = arrow_array.offset
        Py_ssize_t offset_in_bits = arr_offset & 7
        Py_ssize_t offset_in_bytes = arr_offset >> 3

        # Loop variables
        Py_ssize_t i, byte_index, bit_index
        Py_ssize_t start, end, length

    # Fetch raw pointers (if present)
    if len(buffers) > 0 and buffers[0]:
        validity = <const uint8_t*><uintptr_t>(buffers[0].address)
    if len(buffers) > 1 and buffers[1]:
        offsets = <const int32_t*><uintptr_t>(buffers[1].address)
    if len(buffers) > 2 and buffers[2]:
        data = <const char*><uintptr_t>(buffers[2].address)

    # If needle is empty or array empty, everything is 0
    if pattern_length == 0 or n == 0:
        for i in range(n):
            result_view[i] = 0
        return result_view

    # Main loop
    for i in range(n):
        # Default to no match
        result_view[i] = 0

        # Check null bit
        if validity is not NULL:
            byte_index = offset_in_bytes + ((offset_in_bits + i) >> 3)
            bit_index = (offset_in_bits + i) & 7
            if not (validity[byte_index] & (1 << bit_index)):
                continue  # null → 0

        # Calculate string (or binary) boundaries
        start = offsets[arr_offset + i]
        end = offsets[arr_offset + i + 1]
        length = end - start

        if length < pattern_length:
            continue

        # Direct call to case-insensitive BMH
        if boyer_moore_horspool_case_insensitive(
            data + start,
            <size_t>length,
            c_pattern,
            pattern_length
        ):
            result_view[i] = 1

    return result_view


cpdef uint8_t[::1] list_substring_case_insensitive(object column, str needle):
    """
    Perform a case-insensitive substring search on an Arrow column, which may be
    a single Array or a ChunkedArray of strings/binaries. Returns a NumPy uint8
    array (1 for match, 0 for non-match/null).

    Parameters:
        column: object
            Arrow array or ChunkedArray (StringArray/BinaryArray).
        needle: str
            Pattern to find, ignoring case.

    Returns:
        A contiguous numpy.uint8 array of length == sum(len(chunk) for chunk in column).
    """
    cdef:
        Py_ssize_t total_length = 0
        numpy.ndarray[numpy.uint8_t, ndim=1] final_result
        uint8_t[::1] final_view
        Py_ssize_t offset = 0
        uint8_t[::1] chunk_view
        object chunk

    # If it's not chunked, just do the single-array logic
    if not hasattr(column, "chunks"):
        return _substring_in_single_array_case_insensitive(column, needle)

    # Otherwise, handle chunked array
    for chunk in column.chunks:
        total_length += len(chunk)

    final_result = numpy.empty(total_length, dtype=numpy.uint8)
    final_view = final_result

    offset = 0
    for chunk in column.chunks:
        chunk_view = _substring_in_single_array_case_insensitive(chunk, needle)
        final_view[offset : offset + len(chunk)] = chunk_view
        offset += len(chunk)

    return final_view
