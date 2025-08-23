# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdlib cimport malloc, free
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString

from opteryx.third_party.alantsd.base64 cimport b64tobin, bintob64

cdef inline size_t calc_encoded_size(size_t length):
    """Base64-encoded output length (without newlines)."""
    return ((length + 2) // 3) * 4

cdef inline size_t calc_decoded_size(size_t length):
    """Worst-case decoded output size (since we skip padding in-place)."""
    return (length // 4) * 3

cpdef bytes encode(bytes data):
    """
    Base64-encode bytes to a bytes object (null-terminated internally).
    """
    cdef size_t in_len = len(data)
    cdef size_t out_len = calc_encoded_size(in_len)

    cdef char* outbuf = <char*>malloc(out_len + 1)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* input_ptr = PyBytes_AsString(data)
    bintob64(outbuf, <const void*>input_ptr, in_len)

    cdef bytes result = PyBytes_FromStringAndSize(outbuf, out_len)
    free(outbuf)
    return result


cpdef bytes decode(bytes data):
    """
    Base64-decode bytes to a bytes object.
    """
    cdef size_t in_len = len(data)
    cdef size_t out_len = calc_decoded_size(in_len)

    cdef char* outbuf = <char*>malloc(out_len)
    if outbuf == NULL:
        raise MemoryError()

    cdef const char* input_ptr = PyBytes_AsString(data)
    cdef char* end_ptr = <char*>b64tobin(outbuf, input_ptr)

    if end_ptr is NULL or end_ptr < outbuf or end_ptr > outbuf + out_len:
        free(outbuf)
        raise ValueError("Invalid base64 input")

    cdef Py_ssize_t written = end_ptr - outbuf

    result = PyBytes_FromStringAndSize(outbuf, written)
    free(outbuf)
    return result


# Cython-callable versions

cdef bytes cy_encode(const unsigned char[::1] data):
    return encode(bytes(data))

cdef bytes cy_decode(const char[::1] data):
    return decode(bytes(data))
