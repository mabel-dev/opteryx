# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
This is not a general perpose Bloom Filter, if used outside Opteryx it may not
perform entirely as expected as it is optimized for a specific configuration
and constraints.

We have four size options, all using 2 hashes:
    - a 8k slot bit array for up to 1000 items (about 4.9% FPR)
    - a 512k slot bit array for up to 60k items (about 4.2% FPR)
    - a 8m slot bit array for up to 1m items (about 4.5% FPR)
    - a 128m slot bit array for up to 16m items (about 4.7% FPR)

We perform one hash and then use a calculation based on the golden ratio to
determine the second position. This is cheaper than performing two hashes whilst
still providing a good enough split of the two hashes.

The primary use for this structure is to prefilter JOINs, it is many times faster
(about 20x from initial benchmarking) to test for containment in the bloom filter
that to look up the item in the hash table.

Building the filter is fast - for tables up to 1 million records we create the filter
(1m records is roughly a 0.005s build). If the filter isn't effective (less that 5%
eliminations) we discard it which has meant some waste work.

The 16m set is the limit at the moment, it takes about 0.08 seconds to build which
is the limit of what we think we should speculatively build.
"""

from libc.stdlib cimport calloc, free
from libc.stdint cimport uint64_t, uint32_t

from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices

import numpy
cimport numpy

cdef extern from "<stdint.h>":
    ctypedef unsigned long uintptr_t

# Define sizes for the Bloom filters - now in 64-bit chunks
cdef uint32_t BIT64_ARRAY_SIZE_TINY = 128  # 128 * 64 = 8,192 bits
cdef uint32_t BIT64_ARRAY_SIZE_SMALL = 8 * 1024  # 8K * 64 = 524,288 bits
cdef uint32_t BIT64_ARRAY_SIZE_LARGE = 128 * 1024  # 128K * 64 = 8,388,608 bits
cdef uint32_t BIT64_ARRAY_SIZE_HUGE = 2 * 1024 * 1024  # 2M * 64 = 134,217,728 bits

# Golden ratio constant for second hash
cdef uint64_t GOLDEN_RATIO = 0x9E3779B97F4A7C15ULL

cdef class BloomFilter:

    def __cinit__(self, uint32_t expected_records=50000):
        """Initialize Bloom Filter based on expected number of records."""
        if expected_records <= 1_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_TINY
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_TINY * 64
        elif expected_records <= 62_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_SMALL
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_SMALL * 64
        elif expected_records <= 1_000_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_LARGE
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_LARGE * 64
        elif expected_records <= 16_000_000:
            self.bit64_array_size = BIT64_ARRAY_SIZE_HUGE
            self.bit_array_size_bits = BIT64_ARRAY_SIZE_HUGE * 64
        else:
            raise ValueError("Too many records for this Bloom filter implementation")

        # Precompute mask for faster modulo operations
        self.bit_mask = self.bit_array_size_bits - 1

        # Allocate 64-bit aligned memory
        self.bit_array = <uint64_t*>calloc(self.bit64_array_size, sizeof(uint64_t))
        if not self.bit_array:
            raise MemoryError("Failed to allocate memory for the Bloom filter.")

    def __dealloc__(self):
        if self.bit_array:
            free(self.bit_array)

    cdef inline void _add(self, const uint64_t item):
        cdef uint64_t h1, h2

        # Use bit mask for fast modulo (works because sizes are powers of 2)
        h1 = item & self.bit_mask
        # Better hash mixing for second position
        h2 = (item * GOLDEN_RATIO) & self.bit_mask

        # Set bits using 64-bit operations
        self.bit_array[h1 >> 6] |= (<uint64_t>1) << (h1 & 0x3F)
        self.bit_array[h2 >> 6] |= (<uint64_t>1) << (h2 & 0x3F)

    cpdef void add(self, const uint64_t item):
        self._add(item)

    cdef inline bint _possibly_contains(self, const uint64_t item):
        cdef uint64_t h1, h2, mask1, mask2

        h1 = item & self.bit_mask
        h2 = (item * GOLDEN_RATIO) & self.bit_mask

        # Check both bits with single 64-bit load each
        mask1 = (<uint64_t>1) << (h1 & 0x3F)
        mask2 = (<uint64_t>1) << (h2 & 0x3F)

        return (self.bit_array[h1 >> 6] & mask1) != 0 and \
               (self.bit_array[h2 >> 6] & mask2) != 0

    cpdef bint possibly_contains(self, const uint64_t item):
        return self._possibly_contains(item)

    cpdef numpy.ndarray[numpy.npy_bool, ndim=1] possibly_contains_many(self, object relation, list columns):
        """
        Optimized batch checking with better memory access patterns.
        """
        cdef Py_ssize_t num_rows = relation.num_rows
        cdef numpy.ndarray[numpy.npy_bool, ndim=1] result = numpy.zeros(num_rows, dtype=numpy.bool_)
        cdef uint8_t[::1] result_view = result
        cdef int64_t[::1] valid_row_ids = non_null_row_indices(relation, columns)
        cdef Py_ssize_t num_valid_rows = valid_row_ids.shape[0]
        cdef numpy.ndarray[numpy.uint64_t, ndim=1] row_hashes_np = numpy.zeros(num_rows, dtype=numpy.uint64)
        cdef uint64_t[::1] row_hashes = row_hashes_np
        cdef Py_ssize_t i
        cdef int64_t row_id
        cdef uint64_t hash_val, h1, h2, mask1, mask2

        if num_valid_rows == 0:
            return result

        # Compute hashes only for non-null rows
        compute_row_hashes(relation, columns, row_hashes)

        # Precompute constants
        cdef uint64_t bit_mask = self.bit_mask
        cdef uint64_t golden_ratio = GOLDEN_RATIO
        cdef uint64_t* bit_array = self.bit_array

        for i in range(num_valid_rows):
            row_id = valid_row_ids[i]
            hash_val = row_hashes[row_id]

            h1 = hash_val & bit_mask
            h2 = (hash_val * golden_ratio) & bit_mask

            mask1 = (<uint64_t>1) << (h1 & 0x3F)
            mask2 = (<uint64_t>1) << (h2 & 0x3F)

            result_view[row_id] = (bit_array[h1 >> 6] & mask1) != 0 and \
                (bit_array[h2 >> 6] & mask2) != 0

        return result

cpdef BloomFilter create_bloom_filter(object relation, list columns):
    """
    Optimized Bloom filter creation with better cache behavior.
    """
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        int64_t[::1] valid_row_ids = non_null_row_indices(relation, columns)
        Py_ssize_t num_valid_rows = valid_row_ids.shape[0]
        numpy.ndarray[numpy.uint64_t, ndim=1] row_hashes_np = numpy.empty(num_rows, dtype=numpy.uint64)
        uint64_t[::1] row_hashes = row_hashes_np
        Py_ssize_t i
        int64_t row_id
        BloomFilter bf = BloomFilter(num_valid_rows)
        uint64_t hash_val, h1, h2

    if num_valid_rows == 0:
        return bf

    # Populate row hashes using the selected columns
    compute_row_hashes(relation, columns, row_hashes)

    # Precompute constants for faster access
    cdef uint64_t bit_mask = bf.bit_mask
    cdef uint64_t golden_ratio = GOLDEN_RATIO
    cdef uint64_t* bit_array = bf.bit_array

    # Add to bloom filter
    for i in range(num_valid_rows):
        row_id = valid_row_ids[i]
        hash_val = row_hashes[row_id]

        h1 = hash_val & bit_mask
        h2 = (hash_val * golden_ratio) & bit_mask

        bit_array[h1 >> 6] |= (<uint64_t>1) << (h1 & 0x3F)
        bit_array[h2 >> 6] |= (<uint64_t>1) << (h2 & 0x3F)

    return bf
