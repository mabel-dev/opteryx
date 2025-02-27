from libc.stdint cimport uint64_t

cdef void process_column(object column, uint64_t[::1] row_hashes)
