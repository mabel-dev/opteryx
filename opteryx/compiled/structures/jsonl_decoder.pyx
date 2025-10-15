# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Fast JSONL decoder using Cython for performance-critical operations.

This decoder uses native C string operations instead of regex for better performance.
"""

import numpy
cimport numpy
numpy.import_array()

from libc.string cimport memchr, strlen, strstr
from libc.stdlib cimport strtod, strtol, atoi
from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_GET_SIZE
from libc.stdint cimport int64_t

import pyarrow
from opteryx.third_party.tktech import csimdjson as simdjson


cdef inline const char* find_key_value(const char* line, Py_ssize_t line_len, const char* key, Py_ssize_t key_len, Py_ssize_t* value_start, Py_ssize_t* value_len) nogil:
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
    
    # Search for the key pattern: "key":
    while pos < end:
        # Find opening quote of a key
        key_pos = <const char*>memchr(pos, b'"', end - pos)
        if key_pos == NULL:
            return NULL
        
        key_pos += 1  # Move past the opening quote
        
        # Check if this matches our key
        if (end - key_pos >= key_len and 
            memcmp(key_pos, key, key_len) == 0 and
            key_pos[key_len] == b'"'):
            
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
                while quote_end < end:
                    if quote_end[0] == b'"' and (quote_end == quote_start or quote_end[-1] != b'\\'):
                        value_len[0] = (quote_end + 1) - value_pos
                        return value_pos
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
                    if quote_end[0] in (b' ', b',', b'}', b']', b'\t', b'\n'):
                        break
                    quote_end += 1
                value_len[0] = quote_end - value_pos
                return value_pos
        
        pos = key_pos
    
    return NULL


cdef extern from "string.h":
    int memcmp(const void *s1, const void *s2, size_t n)


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
    cdef list column_data = []
    cdef dict result = {}
    cdef Py_ssize_t num_lines = 0
    cdef Py_ssize_t i
    cdef char* end_ptr
    cdef bytes value_bytes
    cdef str value_str
    
    # Initialize column data lists
    for col in column_names:
        column_data.append([])
        result[col] = column_data[-1]
    
    # Count lines first
    cdef const char* newline_pos = pos
    while newline_pos < end:
        newline_pos = <const char*>memchr(newline_pos, b'\n', end - newline_pos)
        if newline_pos == NULL:
            break
        num_lines += 1
        newline_pos += 1
    
    # If last line doesn't end with newline, count it
    if data_len > 0 and data[data_len - 1] != b'\n':
        num_lines += 1
    
    # Process each line
    pos = data
    for i in range(num_lines):
        # Find line end
        line_start = pos
        line_end = <const char*>memchr(line_start, b'\n', end - line_start)
        if line_end == NULL:
            line_end = end
        
        line_len = line_end - line_start
        
        # Skip empty lines
        if line_len == 0:
            pos = line_end + 1 if line_end < end else end
            continue
        
        # Extract each column
        for j, col in enumerate(column_names):
            key_bytes = col.encode('utf-8')
            key_ptr = PyBytes_AS_STRING(key_bytes)
            key_len = PyBytes_GET_SIZE(key_bytes)
            col_type = column_types.get(col, 'str')
            
            value_ptr = find_key_value(line_start, line_len, key_ptr, key_len, &value_start, &value_len)
            
            if value_ptr == NULL:
                # Key not found
                result[col].append(None)
                continue
            
            # Parse value based on type
            if col_type == 'bool':
                if value_len == 4 and memcmp(value_ptr, b"true", 4) == 0:
                    result[col].append(True)
                elif value_len == 5 and memcmp(value_ptr, b"false", 5) == 0:
                    result[col].append(False)
                else:
                    result[col].append(None)
            
            elif col_type == 'int':
                if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                    result[col].append(None)
                else:
                    # Use strtol for integer parsing
                    value_bytes = value_ptr[:value_len]
                    try:
                        result[col].append(int(value_bytes))
                    except ValueError:
                        result[col].append(None)
            
            elif col_type == 'float':
                if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                    result[col].append(None)
                else:
                    # Use strtod for float parsing
                    value_bytes = value_ptr[:value_len]
                    try:
                        result[col].append(float(value_bytes))
                    except ValueError:
                        result[col].append(None)
            
            elif col_type == 'str':
                if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                    result[col].append(None)
                elif value_ptr[0] == b'"':
                    # String value - extract without quotes
                    value_bytes = value_ptr[1:value_len-1]
                    try:
                        value_str = value_bytes.decode('utf-8')
                        # Simple unescape
                        value_str = value_str.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"').replace('\\\\', '\\')
                        result[col].append(value_str)
                    except UnicodeDecodeError:
                        result[col].append(None)
                else:
                    result[col].append(None)
            
            else:
                # For other types (list, dict, null), fall back to Python
                value_bytes = value_ptr[:value_len]
                if value_len == 4 and memcmp(value_ptr, b"null", 4) == 0:
                    result[col].append(None)
                else:
                    import json
                    try:
                        parsed = json.loads(value_bytes.decode('utf-8'))
                        if isinstance(parsed, dict):
                            result[col].append(json.dumps(parsed, ensure_ascii=False))
                        else:
                            result[col].append(parsed)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        result[col].append(None)
        
        # Move to next line
        pos = line_end + 1 if line_end < end else end
    
    return (num_lines, len(column_names), result)
