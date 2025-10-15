# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Fast JSONL decoder using Cython for performance-critical operations.
"""

from libc.string cimport memchr, memcmp
from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_GET_SIZE
from cpython.mem cimport PyMem_Malloc, PyMem_Free

from opteryx.third_party.fastfloat.fast_float cimport c_parse_fast_float

import orjson as json


cdef enum ColumnType:
    COL_BOOL = 0
    COL_INT = 1
    COL_FLOAT = 2
    COL_STR = 3
    COL_OTHER = 4


cdef inline int _column_type_code(str col_type):
    """
    Map column type strings to integer codes for faster comparisons.
    """
    if col_type == 'bool':
        return COL_BOOL
    elif col_type == 'int':
        return COL_INT
    elif col_type == 'float':
        return COL_FLOAT
    elif col_type == 'str':
        return COL_STR
    else:
        return COL_OTHER


cdef inline long long fast_atoll(const char* c_str, Py_ssize_t length) except? -999999999999999:
    """
    Fast C-level string to long long integer conversion.

    This is significantly faster than calling Python's int() function
    as it avoids the Python/C boundary and works directly with char pointers.

    Returns the parsed integer value.
    Raises ValueError if the string contains invalid characters.
    """
    cdef long long value = 0
    cdef int sign = 1
    cdef Py_ssize_t j = 0
    cdef unsigned char c

    if length == 0:
        raise ValueError("Empty string")

    # Check for negative sign
    if c_str[0] == 45:  # ASCII '-'
        sign = -1
        j = 1
    elif c_str[0] == 43:  # ASCII '+'
        j = 1

    if j >= length:
        raise ValueError("Invalid number format")

    # Parse digits
    for j in range(j, length):
        c = c_str[j] - 48  # ASCII '0' is 48
        if c > 9:  # Not a digit (c is unsigned, so < 0 becomes > 9)
            raise ValueError(f"Invalid digit at position {j}")
        value = value * 10 + c

    return sign * value


cdef inline const char* find_key_value(const char* line, Py_ssize_t line_len, const char* key, Py_ssize_t key_len, Py_ssize_t* value_start, Py_ssize_t* value_len):
    """
    Find the value for a given key in a JSON line.

    Returns pointer to value start, or NULL if not found.
    Updates value_start and value_len with the position and length.
    """
    cdef const char* pos = line
    cdef const char* end = line + line_len
    cdef const char* key_pos
    cdef const char* value_pos
    cdef const char* quote_start
    cdef const char* quote_end
    cdef char first_char
    cdef int brace_count
    cdef int bracket_count
    cdef int backslash_run
    cdef Py_ssize_t remaining

    # Search for the key pattern: "key":
    while pos < end:
        # Find opening quote of a key
        remaining = end - pos
        if remaining <= 0:
            return NULL
        key_pos = <const char*>memchr(pos, b'"', <size_t>remaining)
        if key_pos == NULL:
            return NULL

        key_pos += 1  # Move past the opening quote

        # Check if this matches our key
        if (end - key_pos >= key_len and memcmp(key_pos, key, <size_t>key_len) == 0 and key_pos[key_len] == b'"'):

            # Found the key, now find the colon
            value_pos = key_pos + key_len + 1  # Skip closing quote

            # Skip whitespace and colon
            while value_pos < end and (value_pos[0] == b' ' or value_pos[0] == b'\t' or value_pos[0] == b':'):
                value_pos += 1

            if value_pos >= end:
                return NULL

            first_char = value_pos[0]
            value_start[0] = value_pos - line

            # Determine value type and find end
            if first_char == b'"':
                # String value - find closing quote, handling escapes
                quote_start = value_pos + 1
                quote_end = quote_start
                backslash_run = 0
                while quote_end < end:
                    if quote_end[0] == b'\\':
                        backslash_run += 1
                    else:
                        if quote_end[0] == b'"' and (backslash_run & 1) == 0:
                            # Found unescaped quote
                            value_len[0] = (quote_end + 1) - value_pos
                            return value_pos
                        backslash_run = 0
                    quote_end += 1
                return NULL

            elif first_char == b'{':
                # Object - count braces
                brace_count = 1
                quote_end = value_pos + 1
                while quote_end < end and brace_count > 0:
                    if quote_end[0] == b'{':
                        brace_count += 1
                    elif quote_end[0] == b'}':
                        brace_count -= 1
                    elif quote_end[0] == b'"':
                        # Skip string contents to avoid premature brace counting
                        quote_end += 1
                        while quote_end < end:
                            if quote_end[0] == b'\\':
                                quote_end += 2
                                continue
                            if quote_end[0] == b'"':
                                break
                            quote_end += 1
                    quote_end += 1
                value_len[0] = quote_end - value_pos
                return value_pos

            elif first_char == b'[':
                # Array - count brackets
                bracket_count = 1
                quote_end = value_pos + 1
                while quote_end < end and bracket_count > 0:
                    if quote_end[0] == b'[':
                        bracket_count += 1
                    elif quote_end[0] == b']':
                        bracket_count -= 1
                    elif quote_end[0] == b'"':
                        # Skip string contents inside arrays
                        quote_end += 1
                        while quote_end < end:
                            if quote_end[0] == b'\\':
                                quote_end += 2
                                continue
                            if quote_end[0] == b'"':
                                break
                            quote_end += 1
                    quote_end += 1
                value_len[0] = quote_end - value_pos
                return value_pos

            elif first_char == b'n':
                # null
                if end - value_pos >= 4 and memcmp(value_pos, b"null", 4) == 0:
                    value_len[0] = 4
                    return value_pos
                return NULL

            elif first_char == b't':
                # true
                if end - value_pos >= 4 and memcmp(value_pos, b"true", 4) == 0:
                    value_len[0] = 4
                    return value_pos
                return NULL

            elif first_char == b'f':
                # false
                if end - value_pos >= 5 and memcmp(value_pos, b"false", 5) == 0:
                    value_len[0] = 5
                    return value_pos
                return NULL

            else:
                # Number - find end (space, comma, brace, bracket)
                quote_end = value_pos + 1
                while quote_end < end:
                    # Check for delimiter characters
                    if quote_end[0] == b' ' or quote_end[0] == b',' or quote_end[0] == b'}' or quote_end[0] == b']' or quote_end[0] == b'\t' or quote_end[0] == b'\n':
                        value_len[0] = quote_end - value_pos
                        return value_pos
                    quote_end += 1
                value_len[0] = quote_end - value_pos
                return value_pos

        pos = key_pos

    return NULL


cpdef fast_jsonl_decode_columnar(bytes buffer, list column_names, dict column_types, Py_ssize_t sample_size=100):
    """
    Fast JSONL decoder that extracts values using C string operations.

    Parameters:
        buffer: bytes - The JSONL data
        column_names: list - List of column names to extract
        column_types: dict - Dictionary mapping column names to types ('bool', 'int', 'float', 'str', etc.)
        sample_size: int - Number of lines to use for schema inference (not used if column_types provided)

    Returns:
        tuple: (num_rows, num_cols, dict of column_name -> list of values)
    """
    cdef const char* data = PyBytes_AS_STRING(buffer)
    cdef Py_ssize_t data_len = PyBytes_GET_SIZE(buffer)
    cdef const char* line_start
    cdef const char* line_end
    cdef const char* pos = data
    cdef const char* end = data + data_len
    cdef Py_ssize_t line_len
    cdef Py_ssize_t value_start
    cdef Py_ssize_t value_len
    cdef const char* value_ptr
    cdef bytes key_bytes
    cdef const char* key_ptr
    cdef Py_ssize_t key_len
    cdef str col_type
    cdef dict result = {}
    cdef Py_ssize_t num_lines = 0
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
    cdef str value_str
    cdef object parsed
    cdef Py_ssize_t remaining
    cdef const char* newline_pos

    result = {}

    if num_cols > 0:
        type_codes = <int*>PyMem_Malloc(num_cols * sizeof(int))
        key_ptrs = <const char**>PyMem_Malloc(num_cols * sizeof(const char*))
        key_lengths = <Py_ssize_t*>PyMem_Malloc(num_cols * sizeof(Py_ssize_t))
        if type_codes == NULL or key_ptrs == NULL or key_lengths == NULL:
            if type_codes != NULL:
                PyMem_Free(type_codes)
            if key_ptrs != NULL:
                PyMem_Free(key_ptrs)
            if key_lengths != NULL:
                PyMem_Free(key_lengths)
            raise MemoryError()

    try:
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

        while pos < end:
            line_start = pos
            remaining = end - line_start
            if remaining <= 0:
                break
            newline_pos = <const char*>memchr(line_start, b'\n', <size_t>remaining)
            if newline_pos == NULL:
                line_end = end
                pos = end
            else:
                line_end = newline_pos
                pos = newline_pos + 1

            line_len = line_end - line_start
            num_lines += 1

            if line_len == 0:
                for i in range(num_cols):
                    (<list>column_lists[i]).append(None)
                continue

            for i in range(num_cols):
                col_list = <list>column_lists[i]
                key_ptr = key_ptrs[i]
                key_len = key_lengths[i]
                type_code = type_codes[i]

                value_ptr = find_key_value(line_start, line_len, key_ptr, key_len, &value_start, &value_len)

                if value_ptr == NULL:
                    col_list.append(None)
                    continue

                if type_code == COL_BOOL:
                    if value_len == 4 and memcmp(value_ptr, b"true", 4) == 0:
                        col_list.append(True)
                    elif value_len == 5 and memcmp(value_ptr, b"false", 5) == 0:
                        col_list.append(False)
                    else:
                        col_list.append(None)

                elif type_code == COL_INT:
                    if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                        col_list.append(None)
                    else:
                        col_list.append(fast_atoll(value_ptr, value_len))

                elif type_code == COL_FLOAT:
                    if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                        col_list.append(None)
                    else:
                        value_bytes = PyBytes_FromStringAndSize(value_ptr, value_len)
                        col_list.append(c_parse_fast_float(value_bytes))

                elif type_code == COL_STR:
                    if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                        col_list.append(None)
                    elif value_ptr[0] == b'"' and value_len >= 2:
                        value_bytes = PyBytes_FromStringAndSize(value_ptr + 1, value_len - 2)
                        try:
                            value_str = value_bytes.decode('utf-8')
                            value_str = value_str.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"').replace('\\\\', '\\')
                            col_list.append(value_str)
                        except UnicodeDecodeError:
                            col_list.append(None)
                    else:
                        col_list.append(None)

                else:
                    if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                        col_list.append(None)
                    else:
                        value_bytes = PyBytes_FromStringAndSize(value_ptr, value_len)
                        try:
                            parsed = json.loads(value_bytes)
                            if isinstance(parsed, dict):
                                col_list.append(json.dumps(parsed))
                            else:
                                col_list.append(parsed)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            col_list.append(None)

        return (num_lines, num_cols, result)
    finally:
        if type_codes != NULL:
            PyMem_Free(type_codes)
        if key_ptrs != NULL:
            PyMem_Free(key_ptrs)
        if key_lengths != NULL:
            PyMem_Free(key_lengths)


# Declare the C function we need
cdef extern from "Python.h":
    bytes PyBytes_FromStringAndSize(const char *v, Py_ssize_t len)
