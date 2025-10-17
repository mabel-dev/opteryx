# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Optimized Fast JSONL decoder using Cython for performance-critical operations.
"""

from libc.string cimport memcmp
from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_GET_SIZE, PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Malloc, PyMem_Free

from opteryx.third_party.fastfloat.fast_float cimport c_parse_fast_float

import orjson as json
import platform

cdef const char* LIT_NULL = b"null"
cdef const char* LIT_TRUE = b"true"
cdef const char* LIT_FALSE = b"false"

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

cdef enum ColumnType:
    COL_BOOLEAN = 0
    COL_INTEGER = 1
    COL_FLOAT = 2
    COL_BINARY = 3
    COL_OTHER = 4

cdef inline int _column_type_code(str col_type):
    if col_type == 'bool':
        return COL_BOOLEAN
    elif col_type == 'int':
        return COL_INTEGER
    elif col_type == 'float':
        return COL_FLOAT
    elif col_type == 'str':
        return COL_BINARY
    elif col_type == 'dict':
        return COL_BINARY
    else:
        return COL_OTHER

cdef inline long long fast_atoll(const char* c_str, Py_ssize_t length) except? -999999999999999:
    cdef long long value = 0
    cdef int sign = 1
    cdef Py_ssize_t j = 0
    cdef unsigned char c

    if length == 0:
        raise ValueError("Empty string")

    # Handle sign
    if c_str[0] == 45:  # '-'
        sign = -1
        j = 1
    elif c_str[0] == 43:  # '+'
        j = 1

    if j >= length:
        raise ValueError("Invalid number format")

    # Parse digits
    for j in range(j, length):
        c = c_str[j] - 48  # '0'
        if c > 9:
            raise ValueError(f"Invalid digit at position {j}")
        value = value * 10 + c

    return sign * value

# Optimized: Single pass value extraction for all columns
cdef inline void extract_all_values(
        const char* line, Py_ssize_t line_len,
        const char** key_ptrs, Py_ssize_t* key_lengths,
        int num_cols, const char** value_ptrs,
        Py_ssize_t* value_lens, int* found_flags):
    """
    Extract values for all columns in a single pass through the line.
    """
    cdef const char* pos = line
    cdef const char* end = line + line_len
    cdef const char* key_start
    cdef const char* value_start
    cdef Py_ssize_t remaining
    cdef int quote_offset
    cdef int i
    cdef int found_count = 0

    while pos < end and found_count < num_cols:
        # Find opening quote for key
        remaining = end - pos
        if remaining <= 0:
            break

        quote_offset = simd_search(pos, <size_t>remaining, 34)  # '"'
        if quote_offset == -1:
            break

        key_start = pos + quote_offset + 1  # Skip opening quote

        # Find closing quote for key
        remaining = end - key_start
        if remaining <= 0:
            break

        quote_offset = simd_search(key_start, <size_t>remaining, 34)
        if quote_offset == -1:
            break

        # Check if this key matches any of our columns
        key_len = quote_offset
        for i in range(num_cols):
            if not found_flags[i] and key_lengths[i] == key_len:
                if memcmp(key_start, key_ptrs[i], <size_t>key_len) == 0:
                    # Found matching key, extract value
                    value_start = key_start + key_len + 1  # Skip closing quote

                    # Skip whitespace and colon
                    while value_start < end and (value_start[0] in (32, 9, 13, 58)):
                        value_start += 1

                    if value_start >= end:
                        found_flags[i] = 1
                        found_count += 1
                        break

                    # Extract value
                    if extract_value(value_start, end - value_start, &value_ptrs[i], &value_lens[i]):
                        found_flags[i] = 1
                        found_count += 1
                    break

        pos = key_start + key_len + 1

cdef inline int extract_value(
        const char* value_start, Py_ssize_t remaining,
        const char** value_ptr, Py_ssize_t* value_len):
    """
    Extract value starting at value_start, returning pointer and length.
    Returns 1 on success, 0 on failure.
    """
    cdef const char* pos = value_start
    cdef const char* end = value_start + remaining
    cdef char first_char
    cdef int brace_count, bracket_count, backslash_run
    cdef int offset

    if pos >= end:
        return 0

    first_char = pos[0]

    if first_char == 34:  # '"' - string
        pos += 1  # Skip opening quote
        backslash_run = 0
        while pos < end:
            if pos[0] == 92:  # '\\'
                backslash_run += 1
            else:
                if pos[0] == 34 and (backslash_run & 1) == 0:  # Unescaped quote
                    value_ptr[0] = value_start
                    value_len[0] = (pos + 1) - value_start
                    return 1
                backslash_run = 0
            pos += 1
        return 0

    elif first_char == 123:  # '{' - object
        brace_count = 1
        pos += 1
        while pos < end and brace_count > 0:
            if pos[0] == 123:
                brace_count += 1
            elif pos[0] == 125:
                brace_count -= 1
            elif pos[0] == 34:  # Skip strings within object
                pos += 1
                while pos < end:
                    if pos[0] == 92:
                        pos += 2
                        continue
                    if pos[0] == 34:
                        break
                    pos += 1
            pos += 1
        value_ptr[0] = value_start
        value_len[0] = pos - value_start
        return 1

    elif first_char == 91:  # '[' - array
        bracket_count = 1
        pos += 1
        while pos < end and bracket_count > 0:
            if pos[0] == 91:
                bracket_count += 1
            elif pos[0] == 93:
                bracket_count -= 1
            elif pos[0] == 34:  # Skip strings within array
                pos += 1
                while pos < end:
                    if pos[0] == 92:
                        pos += 2
                        continue
                    if pos[0] == 34:
                        break
                    pos += 1
            pos += 1
        value_ptr[0] = value_start
        value_len[0] = pos - value_start
        return 1

    else:  # Number, boolean, or null
        # Use SIMD to find delimiter
        if remaining > 1:
            offset = simd_find_delimiter(value_start + 1, <size_t>(remaining - 1))
            if offset >= 0:
                value_ptr[0] = value_start
                value_len[0] = (value_start + 1 + offset) - value_start
                return 1

        # No delimiter found, value extends to end
        value_ptr[0] = value_start
        value_len[0] = remaining
        return 1

# Optimized float parsing without intermediate bytes
cdef inline double parse_float_direct(const char* value_ptr, Py_ssize_t value_len) except? -1.0:
    """
    Parse float directly from C string without creating intermediate bytes object.
    """
    cdef bytes value_bytes = PyBytes_FromStringAndSize(value_ptr, value_len)
    return c_parse_fast_float(value_bytes)

cpdef fast_jsonl_decode_columnar(bytes buffer, list column_names, dict column_types, Py_ssize_t sample_size=100):
    cdef const char* data = PyBytes_AS_STRING(buffer)
    cdef Py_ssize_t data_len = PyBytes_GET_SIZE(buffer)
    cdef const char* line_start
    cdef const char* line_end
    cdef const char* pos = data
    cdef const char* end = data + data_len
    cdef Py_ssize_t line_len
    cdef str col_type
    cdef dict result = {}
    cdef Py_ssize_t i
    cdef Py_ssize_t num_cols = len(column_names)
    cdef list column_lists = []
    cdef list key_bytes_list = []
    cdef int* type_codes = NULL
    cdef const char** key_ptrs = NULL
    cdef Py_ssize_t* key_lengths = NULL
    cdef int type_code
    cdef list col_list
    cdef bytes value_bytes
    cdef object parsed
    cdef Py_ssize_t remaining
    cdef size_t line_count
    cdef size_t estimated_lines
    cdef Py_ssize_t line_index = 0
    cdef int newline_offset

    # Buffers for single-pass extraction
    cdef const char** value_ptrs = NULL
    cdef Py_ssize_t* value_lens = NULL
    cdef int* found_flags = NULL

    result = {}

    if num_cols > 0:
        # Allocate memory for column metadata
        type_codes = <int*>PyMem_Malloc(num_cols * sizeof(int))
        key_ptrs = <const char**>PyMem_Malloc(num_cols * sizeof(const char*))
        key_lengths = <Py_ssize_t*>PyMem_Malloc(num_cols * sizeof(Py_ssize_t))
        value_ptrs = <const char**>PyMem_Malloc(num_cols * sizeof(const char*))
        value_lens = <Py_ssize_t*>PyMem_Malloc(num_cols * sizeof(Py_ssize_t))
        found_flags = <int*>PyMem_Malloc(num_cols * sizeof(int))

        if (
                type_codes == NULL or key_ptrs == NULL or key_lengths == NULL or
                value_ptrs == NULL or value_lens == NULL or found_flags == NULL):
            # Cleanup on allocation failure
            if type_codes != NULL:
                PyMem_Free(type_codes)
            if key_ptrs != NULL:
                PyMem_Free(key_ptrs)
            if key_lengths != NULL:
                PyMem_Free(key_lengths)
            if value_ptrs != NULL:
                PyMem_Free(value_ptrs)
            if value_lens != NULL:
                PyMem_Free(value_lens)
            if found_flags != NULL:
                PyMem_Free(found_flags)
            raise MemoryError()

    try:
        # Initialize column metadata
        for i in range(num_cols):
            col = column_names[i]
            key_bytes = col.encode('utf-8')
            key_bytes_list.append(key_bytes)
            key_ptrs[i] = PyBytes_AS_STRING(key_bytes)
            key_lengths[i] = PyBytes_GET_SIZE(key_bytes)
            col_list = []
            column_lists.append(col_list)
            result[col] = col_list
            col_type = column_types.get(col, 'str')
            type_codes[i] = _column_type_code(col_type)

        # Pre-count lines for preallocation
        line_count = simd_count(data, data_len, 10)  # Count '\n'
        if data_len > 0 and data[data_len - 1] != 10:
            estimated_lines = line_count + 1
        else:
            estimated_lines = line_count

        # Preallocate column lists
        for i in range(num_cols):
            col_list = [None] * estimated_lines
            column_lists[i] = col_list
            result[column_names[i]] = col_list

        # Main processing loop
        while pos < end:
            line_start = pos
            remaining = end - line_start
            if remaining <= 0:
                break

            # Find line end
            newline_offset = simd_search(line_start, <size_t>remaining, 10)  # '\n'
            if newline_offset == -1:
                line_end = end
                pos = end
            else:
                line_end = line_start + newline_offset
                pos = line_end + 1

            line_len = line_end - line_start

            if line_len == 0:
                line_index += 1
                continue

            # Reset found flags and value pointers for this line
            for i in range(num_cols):
                found_flags[i] = 0
                value_ptrs[i] = NULL
                value_lens[i] = 0

            # Single-pass extraction for all columns
            extract_all_values(
                line_start, line_len, key_ptrs, key_lengths,
                num_cols, value_ptrs, value_lens, found_flags
            )

            # Process extracted values
            for i in range(num_cols):
                if not found_flags[i] or value_ptrs[i] == NULL:
                    continue  # Already None

                col_list = <list>column_lists[i]
                type_code = type_codes[i]
                value_ptr = value_ptrs[i]
                value_len = value_lens[i]

                # Handle null
                if value_len == 4 and memcmp(value_ptr, LIT_NULL, 4) == 0:
                    continue  # Already None

                if type_code == COL_BOOLEAN:
                    if value_len == 4 and memcmp(value_ptr, LIT_TRUE, 4) == 0:
                        col_list[line_index] = True
                    elif value_len == 5 and memcmp(value_ptr, LIT_FALSE, 5) == 0:
                        col_list[line_index] = False

                elif type_code == COL_INTEGER:
                    try:
                        col_list[line_index] = fast_atoll(value_ptr, value_len)
                    except ValueError:
                        pass  # Keep as None

                elif type_code == COL_FLOAT:
                    try:
                        col_list[line_index] = parse_float_direct(value_ptr, value_len)
                    except (ValueError, TypeError):
                        pass  # Keep as None

                elif type_code == COL_BINARY:
                    # For strings, remove quotes; for objects, keep as-is
                    if value_ptr[0] == 34 and value_len >= 2:  # String
                        col_list[line_index] = PyBytes_FromStringAndSize(value_ptr + 1, value_len - 2)
                    else:  # Object or other
                        col_list[line_index] = PyBytes_FromStringAndSize(value_ptr, value_len)

                else:  # COL_OTHER
                    value_bytes = PyBytes_FromStringAndSize(value_ptr, value_len)
                    try:
                        parsed = json.loads(value_bytes)
                        if isinstance(parsed, dict):
                            col_list[line_index] = json.dumps(parsed)
                        else:
                            col_list[line_index] = parsed
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass  # Keep as None

            line_index += 1

        # Trim column lists to actual size
        for i, col in enumerate(column_names):
            col_list = column_lists[i]
            if len(col_list) != line_index:
                del col_list[line_index:]

        return (line_index, num_cols, result)

    finally:
        # Cleanup
        if type_codes != NULL:
            PyMem_Free(type_codes)
        if key_ptrs != NULL:
            PyMem_Free(key_ptrs)
        if key_lengths != NULL:
            PyMem_Free(key_lengths)
        if value_ptrs != NULL:
            PyMem_Free(value_ptrs)
        if value_lens != NULL:
            PyMem_Free(value_lens)
        if found_flags != NULL:
            PyMem_Free(found_flags)
