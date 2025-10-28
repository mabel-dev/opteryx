# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Optimized regex replacement implementation for vectorized string operations.

This module provides a high-performance implementation of regex replace operations
for arrays of strings, optimized for the common case where the pattern and replacement
are constant across all strings (as in GROUP BY clauses).
"""

import re
import threading
import numpy
cimport numpy
numpy.import_array()

# Cache for compiled regex patterns to avoid recompiling the same pattern
# This is especially beneficial when the same pattern is used multiple times
# in a query (e.g., in both SELECT and GROUP BY clauses)
# Thread-safe using a lock
_pattern_cache = {}
_cache_lock = threading.Lock()


cpdef numpy.ndarray list_regex_replace(numpy.ndarray data, object pattern, object replacement):
    """
    Vectorized regex replace implementation optimized for constant patterns.
    
    This is significantly faster than PyArrow's replace_substring_regex for large
    datasets because:
    1. It compiles the regex pattern once and reuses it
    2. Uses efficient C-level loops via Cython
    3. Avoids Arrow array conversion overhead
    4. Optimized for the common case of constant pattern/replacement
    
    Parameters
    ----------
    data : numpy.ndarray
        Array of strings to perform regex replacement on
    pattern : str or bytes
        Regular expression pattern to match
    replacement : str or bytes
        Replacement string (can use backreferences like \\1, \\2, etc.)
    
    Returns
    -------
    numpy.ndarray
        Array with regex replacements applied
    """
    cdef Py_ssize_t length = data.shape[0]
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(length, dtype=object)
    
    cdef Py_ssize_t i
    cdef object value
    cdef object compiled_pattern
    cdef bint is_bytes_mode = False
    
    # Handle empty input
    if length == 0:
        return result
    
    # Determine if we're working with bytes or strings
    # Check the pattern type
    cdef object cache_key
    if isinstance(pattern, bytes):
        is_bytes_mode = True
        cache_key = pattern
    else:
        cache_key = str(pattern)
    
    # Check cache for compiled pattern (thread-safe)
    with _cache_lock:
        if cache_key in _pattern_cache:
            compiled_pattern = _pattern_cache[cache_key]
        else:
            # Compile pattern outside the critical section for better concurrency
            compiled_pattern = None
    
    # Compile pattern if not found in cache
    if compiled_pattern is None:
        compiled_pattern = re.compile(cache_key)
        # Add to cache with thread safety
        with _cache_lock:
            # Check cache size and add only if there's room
            # This prevents unbounded memory growth
            if len(_pattern_cache) < 100:
                _pattern_cache[cache_key] = compiled_pattern
    
    # Ensure replacement is the right type
    cdef object replacement_val
    if is_bytes_mode:
        if isinstance(replacement, bytes):
            replacement_val = replacement
        else:
            replacement_val = str(replacement).encode('utf-8')
    else:
        replacement_val = str(replacement) if not isinstance(replacement, str) else replacement
    
    # Process each string
    for i in range(length):
        value = data[i]
        
        if value is None:
            result[i] = None
            continue
        
        try:
            if is_bytes_mode:
                # Bytes mode
                if isinstance(value, bytes):
                    result[i] = compiled_pattern.sub(replacement_val, value)
                else:
                    # Convert string to bytes if needed
                    value_bytes = str(value).encode('utf-8')
                    result[i] = compiled_pattern.sub(replacement_val, value_bytes)
            else:
                # String mode
                if isinstance(value, bytes):
                    # Convert bytes to string if needed
                    value_str = value.decode('utf-8')
                    result[i] = compiled_pattern.sub(replacement_val, value_str)
                else:
                    result[i] = compiled_pattern.sub(replacement_val, str(value))
        except Exception:
            # If regex replacement fails for any reason, return the original value
            result[i] = value
    
    return result


def clear_regex_cache():
    """
    Clear the compiled regex pattern cache.
    
    This can be called to free memory if many different patterns have been used.
    The cache is automatically limited to 100 patterns, but this allows manual clearing.
    
    Thread-safe operation.
    """
    global _pattern_cache
    with _cache_lock:
        _pattern_cache.clear()
