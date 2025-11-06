# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from draken.vectors.string_vector cimport (
    _StringVectorCIterator,
    StringElement,
    StringVector,
    StringVectorBuilder,
)

from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_AsStringAndSize
from libcpp.string cimport string

import sys

cdef extern from "re2/stringpiece.h" namespace "re2":
    cdef cppclass StringPiece:
        StringPiece() except +
        StringPiece(const string& other) except +
        StringPiece(const char* data, size_t length) except +

cdef extern from "re2/re2.h" namespace "re2":
    cdef cppclass RE2:
        RE2(const string& pattern) except +
        bint ok() const
        const string& error() const

        @staticmethod
        int GlobalReplace(string* target, const RE2& re, const StringPiece& rewrite)

cpdef StringVector list_regex_replace(
    StringVector data, bytes pattern, bytes replacement
):
    """Optimized regex replace returning a native Draken ``StringVector``.

    Performance optimizations:
    - No Python bytes object created per element
    - Direct pointer access to Draken internal buffers
    - Zero-copy append via append_bytes(ptr, length)
    - Reused C++ string buffer to minimize allocations
    - Source-aware builder sizing to reduce reallocations
    - Single-pass processing with C-level iterator
    """
    cdef Py_ssize_t length = data.ptr.length
    cdef Py_ssize_t i

    # Compile RE2 pattern once
    cdef char* pattern_buf = <char*>0
    cdef char* repl_buf = <char*>0
    cdef Py_ssize_t pattern_len = 0
    cdef Py_ssize_t repl_len = 0
    cdef RE2* regex = NULL
    cdef StringPiece repl_piece
    cdef string pattern_str
    cdef string repl_str

    # For processing elements
    cdef StringElement elem
    cdef string value_str  # Reused buffer for each element
    cdef _StringVectorCIterator it  # C-level iterator
    cdef StringVectorBuilder builder
    cdef Py_ssize_t growth_slack = 0
    cdef Py_ssize_t delta
    cdef double growth_factor = 1.25
    cdef Py_ssize_t max_ssize = sys.maxsize

    # Pre-allocate string buffer with generous capacity to reduce reallocations
    # Most regex operations don't expand strings significantly, so 1024 bytes
    # should handle common cases without reallocation
    value_str.reserve(1024)

    # Extract pattern and replacement buffers
    PyBytes_AsStringAndSize(pattern, &pattern_buf, &pattern_len)
    PyBytes_AsStringAndSize(replacement, &repl_buf, &repl_len)

    pattern_str = string(pattern_buf, <size_t>pattern_len)
    repl_str = string(repl_buf, <size_t>repl_len)

    # Compile regex pattern once (RE2 is thread-safe for reads)
    regex = new RE2(pattern_str)
    repl_piece = StringPiece(repl_str)

    if not regex.ok():
        del regex
        raise ValueError("Invalid regular expression")

    if pattern_len == 0:
        growth_factor = 2.0

    if repl_len > pattern_len and length > 0:
        delta = repl_len - pattern_len
        if delta > 0 and delta <= max_ssize // length:
            growth_slack = delta * length

    try:
        # Create builder using source-aware heuristics to minimize reallocations
        builder = StringVectorBuilder.from_source(data, growth_factor, growth_slack)

        # Get C-level iterator (zero Python object creation overhead)
        it = data.c_iter()

        # Process all elements at C level - tight loop with minimal overhead
        while it.next(&elem):
            if elem.is_null:
                builder.append_null()
            else:
                # Assign directly to reused buffer
                # This reuses the existing capacity when possible
                value_str.assign(elem.ptr, <size_t>elem.length)

                # RE2 performs in-place replacement (modifies value_str)
                # Returns number of replacements made
                RE2.GlobalReplace(&value_str, regex[0], repl_piece)

                # Append the (potentially modified) string to the builder
                builder.append_bytes(value_str.c_str(), value_str.size())

        # Build and return as Draken StringVector
        return builder.finish()

    finally:
        # Clean up regex object
        del regex
