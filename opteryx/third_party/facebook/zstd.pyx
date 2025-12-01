# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.buffer cimport PyBUF_CONTIG_RO, PyBuffer_Release, PyObject_GetBuffer
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.unicode cimport PyUnicode_FromString
from libc.stddef cimport size_t
from libc.stdint cimport uint8_t


cdef extern from "zstd.h":
    size_t ZSTD_decompressBound(const void* src, size_t srcSize)
    size_t ZSTD_decompress(void* dst, size_t dstCapacity, const void* src, size_t srcSize)
    bint ZSTD_isError(size_t code)
    const char* ZSTD_getErrorName(size_t code)
    unsigned long long ZSTD_findDecompressedSize(const void* src, size_t srcSize)
    unsigned long long ZSTD_CONTENTSIZE_ERROR
    unsigned long long ZSTD_CONTENTSIZE_UNKNOWN

cpdef bytes decompress(object data):
    """Decompress a buffer compressed with the vendored zstd sources."""
    cdef Py_buffer view
    cdef size_t dest_capacity
    cdef const uint8_t* src
    cdef size_t src_size
    cdef unsigned long long known_size
    cdef size_t result
    cdef void* dst

    if PyObject_GetBuffer(data, &view, PyBUF_CONTIG_RO) != 0:
        raise TypeError("expected a buffer-like object")

    try:
        if view.len == 0:
            return b""

        src = <const uint8_t*>view.buf
        src_size = <size_t>view.len

        known_size = ZSTD_findDecompressedSize(src, src_size)
        if known_size not in (ZSTD_CONTENTSIZE_ERROR, ZSTD_CONTENTSIZE_UNKNOWN):
            dest_capacity = <size_t>known_size
        else:
            dest_capacity = ZSTD_decompressBound(src, src_size)
            if dest_capacity == 0 or ZSTD_isError(dest_capacity):
                raise ValueError("failed to determine vendored zstd decompress bound")

        if dest_capacity == 0:
            dest_capacity = 1

        dst = PyMem_Malloc(dest_capacity)
        if not dst:
            raise MemoryError()

        result = ZSTD_decompress(dst, dest_capacity, src, src_size)
        if ZSTD_isError(result):
            err_ptr = ZSTD_getErrorName(result)
            PyMem_Free(dst)
            if err_ptr:
                raise ValueError(PyUnicode_FromString(err_ptr))
            raise ValueError("zstd decompression failed")

        py_bytes = PyBytes_FromStringAndSize(<char*>dst, result)
        PyMem_Free(dst)
        if py_bytes is None:
            raise MemoryError()
        return py_bytes
    finally:
        PyBuffer_Release(&view)
