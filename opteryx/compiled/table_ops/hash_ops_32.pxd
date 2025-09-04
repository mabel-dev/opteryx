# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
This 32 bit variation is slightly faster than the 64bit version but has less entropy.

We use this for the bloomfilter, where we mod the values to fit into a smaller space.

We intend to use this for the shuffle.
"""

from libc.stdint cimport uint32_t

cdef void process_column(object column, uint32_t[::1] row_hashes)
cdef void compute_row_hashes(object table, list columns, uint32_t[::1] row_hashes)