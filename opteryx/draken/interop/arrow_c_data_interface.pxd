# arrow_c_data_interface.pxd

from libc.stdint cimport int64_t

cdef extern from "arrow_c_data_interface.h":

    cdef int ARROW_FLAG_DICTIONARY_ORDERED
    cdef int ARROW_FLAG_NULLABLE
    cdef int ARROW_FLAG_MAP_KEYS_SORTED

    cdef struct ArrowSchema:
        const char* format
        const char* name
        const char* metadata
        int64_t flags
        int64_t n_children
        ArrowSchema** children
        ArrowSchema* dictionary
        void (*release)(ArrowSchema*)
        void* private_data

    cdef struct ArrowArray:
        int64_t length
        int64_t null_count
        int64_t offset
        int64_t n_buffers
        int64_t n_children
        const void** buffers
        ArrowArray** children
        ArrowArray* dictionary
        void (*release)(ArrowArray*)
        void* private_data