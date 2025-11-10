# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc, free


def and_mask(bytes a, bytes b, Py_ssize_t n):
    """Return the byte-wise AND of `a` and `b` for `n` bytes as bytes."""
    cdef uint8_t* pa = <uint8_t*> a
    cdef uint8_t* pb = <uint8_t*> b
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()
    cdef Py_ssize_t i
    for i in range(n):
        out[i] = pa[i] & pb[i]
    py_out = bytes(<char *><void*> out, n)
    free(out)
    return py_out


def or_mask(bytes a, bytes b, Py_ssize_t n):
    cdef uint8_t* pa = <uint8_t*> a
    cdef uint8_t* pb = <uint8_t*> b
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()
    cdef Py_ssize_t i
    for i in range(n):
        out[i] = pa[i] | pb[i]
    py_out = bytes(<char *><void*> out, n)
    free(out)
    return py_out


def xor_mask(bytes a, bytes b, Py_ssize_t n):
    cdef uint8_t* pa = <uint8_t*> a
    cdef uint8_t* pb = <uint8_t*> b
    cdef uint8_t* out = <uint8_t*> malloc(n)
    if out == NULL:
        raise MemoryError()
    cdef Py_ssize_t i
    for i in range(n):
        out[i] = pa[i] ^ pb[i]
    py_out = bytes(<char *><void*> out, n)
    free(out)
    return py_out
