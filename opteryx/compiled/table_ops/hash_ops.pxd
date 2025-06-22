# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t

cdef void process_column(object column, uint64_t[::1] row_hashes)
cdef void compute_row_hashes(object table, list columns, uint64_t[::1] row_hashes)