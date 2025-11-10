# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Variable-width buffer helpers used by Draken vector implementations.

This header declares inline C helpers to create, free, and inspect
`DrakenVarBuffer` instances:

- alloc_var_buffer(dtype, length, bytes_cap): allocate header, offsets, and data buffer.
  Offsets array is (length+1) entries; data buffer capacity is bytes_cap.
  Raises MemoryError on OOM.
- free_var_buffer(buf, owns_data): conditionally frees data, offsets, and null bitmap
  based on ownership, then releases the header.
- buf_length(buf): fast accessor for number of entries.

The null_bitmap is not allocated here; producers may set it to a valid bitmap
or NULL. These helpers are consumed by variable-width vectors (e.g., StringVector).
"""

from libc.stdint cimport int32_t, uint8_t
from libc.stdlib cimport free, malloc

from opteryx.draken.core.buffers cimport DrakenVarBuffer
from opteryx.draken.core.buffers cimport DrakenType

cdef inline DrakenVarBuffer* alloc_var_buffer(DrakenType dtype, size_t length, size_t bytes_cap):
    cdef DrakenVarBuffer* buf = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
    if buf == NULL:
        raise MemoryError()

    # allocate offsets: length + 1
    buf.offsets = <int32_t*> malloc((length + 1) * sizeof(int32_t))
    if buf.offsets == NULL:
        free(buf)
        raise MemoryError()

    # allocate data buffer
    if bytes_cap > 0:
        buf.data = <uint8_t*> malloc(bytes_cap)
        if buf.data == NULL:
            free(buf.offsets)
            free(buf)
            raise MemoryError()
    else:
        buf.data = NULL

    buf.null_bitmap = NULL
    buf.length = length
    buf.type = dtype
    return buf

cdef inline void free_var_buffer(DrakenVarBuffer* buf, bint owns_data):
    if buf != NULL:
        if owns_data:
            if buf.data != NULL:
                free(buf.data)
            if buf.offsets != NULL:
                free(buf.offsets)
        if buf.null_bitmap != NULL:
            free(buf.null_bitmap)
        free(buf)

cdef inline size_t buf_length(DrakenVarBuffer* buf):
    return buf.length

cdef inline int buf_dtype(DrakenVarBuffer* buf):
    return <int>buf.type
