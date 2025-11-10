# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from opteryx.draken.vectors.string_vector cimport _StringVectorCIterator, StringElement
from opteryx.draken.vectors import string_vector as string_vector_module

from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_AsStringAndSize
from libcpp.string cimport string

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

cpdef object list_regex_replace(object data, bytes pattern, bytes replacement):
    """Top-level wrapper for list regex replace using Draken C-level iterator.

    Uses Draken 0.3.0b80+ C-level iterator and append_bytes() to eliminate
    Python object creation overhead. The entire operation happens at C level
    except for the initial setup and final conversion.

    Performance improvements vs Python iterator:
    - No Python bytes object created per element
    - Direct pointer access to Draken internal buffers
    - Zero-copy append via append_bytes(ptr, length)
    """
    cdef Py_ssize_t length = data.length

    # Compile RE2 pattern once
    cdef char* pattern_buf = <char*>0
    cdef char* repl_buf = <char*>0
    cdef Py_ssize_t pattern_len = 0
    cdef Py_ssize_t repl_len = 0
    cdef RE2* regex
    cdef StringPiece repl_piece
    cdef string pattern_str
    cdef string repl_str

    # For processing elements
    cdef StringElement elem
    cdef string value_str  # Reused buffer for each element
    cdef _StringVectorCIterator it  # C-level iterator
    cdef object builder  # StringVectorBuilder
    cdef Py_ssize_t estimated_bytes_per_entry = 50

    # Pre-allocate string buffer to reduce reallocation overhead
    # This capacity will grow if needed but helps for common case
    value_str.reserve(256)

    # Extract pattern and replacement buffers
    PyBytes_AsStringAndSize(pattern, &pattern_buf, &pattern_len)
    PyBytes_AsStringAndSize(replacement, &repl_buf, &repl_len)

    pattern_str = string(pattern_buf, <size_t>pattern_len)
    repl_str = string(repl_buf, <size_t>repl_len)

    regex = new RE2(pattern_str)
    repl_piece = StringPiece(repl_str)

    if not regex.ok():
        del regex
        raise ValueError("Invalid regular expression")

    try:
        # Create builder with estimated capacity
        builder = string_vector_module.StringVectorBuilder.with_estimate(length, estimated_bytes_per_entry)

        # Get C-level iterator
        it = data.c_iter()

        # Process all elements at C level
        while it.next(&elem):
            if elem.is_null:
                builder.append_null()
            else:
                # Reuse string buffer - assign() may reuse capacity
                value_str.assign(elem.ptr, <size_t>elem.length)

                # RE2 performs in-place replacement
                RE2.GlobalReplace(&value_str, regex[0], repl_piece)

                # Append directly from C++ string to Draken builder (zero-copy from builder's perspective)
                builder.append_bytes(value_str.c_str(), value_str.size())

        # Build and return as PyArrow array
        return builder.finish().to_arrow()
    finally:
        del regex
