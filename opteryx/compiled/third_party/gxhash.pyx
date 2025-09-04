# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint32_t
from cpython.buffer cimport Py_buffer, PyBUF_SIMPLE, PyObject_GetBuffer, PyBuffer_Release

cdef extern from "gxhash.h":
    uint32_t gx_hash_32(const void* data, size_t length)


def gxhash_32(bytes data) -> int:
    """
    Hash a bytes-like object with gxhash32 using seed=0.
    Returns a 32-bit unsigned integer.
    """
    cdef Py_buffer view
    cdef uint32_t result

    if PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) != 0:
        raise ValueError("Could not acquire buffer.")

    try:
        result = gx_hash_32(view.buf, view.len)
    finally:
        PyBuffer_Release(&view)

    return result
