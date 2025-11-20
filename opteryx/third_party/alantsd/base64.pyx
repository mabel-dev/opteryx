# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdlib cimport malloc, free
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString

from opteryx.third_party.alantsd.base64 cimport b64tobin_len, bintob64, b64_has_neon, b64_has_avx2

cdef inline size_t calc_encoded_size(size_t length):
    """Base64-encoded output length (without newlines)."""
    return ((length + 2) // 3) * 4

cdef inline size_t calc_decoded_size(size_t length):
    """Worst-case decoded output size (since we skip padding in-place)."""
    return (length // 4) * 3


cpdef bytes encode(bytes data):
    """
    Base64-encode a bytes object using bintob64 from C.
    Returns: encoded bytes (not null-terminated).
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
    cdef size_t in_len = len(data)
    cdef size_t out_len = (in_len // 4) * 3  # may be smaller depending on padding

    result = PyBytes_FromStringAndSize(NULL, out_len)
    cdef char* outbuf = PyBytes_AsString(result)
    cdef const char* inbuf = PyBytes_AsString(data)

    cdef char* end_ptr = <char*>b64tobin_len(outbuf, inbuf, in_len)
    if end_ptr == NULL or end_ptr < outbuf or end_ptr > outbuf + out_len:
        return b""

    return result[:end_ptr - outbuf]


cpdef bint has_neon():
    """Check if NEON SIMD is available."""
    return b64_has_neon() != 0


cpdef bint has_avx2():
    """Check if AVX2 SIMD is available."""
    return b64_has_avx2() != 0
