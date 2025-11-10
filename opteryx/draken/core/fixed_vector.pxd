# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Fixed-width buffer helpers used by Draken vector implementations.

This header declares small inline C helpers to create, free, and inspect
`DrakenFixedBuffer` instances:

- alloc_fixed_buffer(dtype, length, itemsize): allocate header and data buffer
  (skips data allocation when length==0 or itemsize==0); raises MemoryError on OOM.
- free_fixed_buffer(buf, owns_data): conditionally frees data and null bitmap
  based on ownership, then releases the header.
- buf_length(buf) / buf_itemsize(buf) / buf_dtype(buf): fast metadata accessors.

The null_bitmap is not allocated here; producers may set it to a valid bitmap
or NULL. These helpers are consumed by fixed-width vectors (e.g., Int64Vector)
to centralize allocation and lifetime management.
"""

from libc.stdint cimport uint8_t
from libc.stdlib cimport free
from libc.stdlib cimport malloc

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DrakenType

cdef inline DrakenFixedBuffer* alloc_fixed_buffer(DrakenType dtype, size_t length, size_t itemsize):
    cdef DrakenFixedBuffer* buf = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if buf == NULL:
        raise MemoryError()
    buf.data = malloc(length * itemsize) if itemsize > 0 and length > 0 else NULL
    if length > 0 and itemsize > 0 and buf.data == NULL:
        free(buf)
        raise MemoryError()
    buf.null_bitmap = NULL
    buf.length = length
    buf.itemsize = itemsize
    buf.type = dtype
    return buf

cdef inline void free_fixed_buffer(DrakenFixedBuffer* buf, bint owns_data):
    if buf != NULL:
        if owns_data and buf.data != NULL:
            free(buf.data)
        if buf.null_bitmap != NULL:
            free(buf.null_bitmap)
        free(buf)

cdef inline size_t buf_length(DrakenFixedBuffer* buf):
    return buf.length

cdef inline size_t buf_itemsize(DrakenFixedBuffer* buf):
    return buf.itemsize

cdef inline int buf_dtype(DrakenFixedBuffer* buf):
    return <int>buf.type
