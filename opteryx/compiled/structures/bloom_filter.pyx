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
from libc.stdint cimport uint8_t

from opteryx.compiled.table_ops.hash_ops cimport compute_row_hashes
from opteryx.compiled.table_ops.null_avoidant_ops cimport non_null_row_indices

import numpy
cimport numpy

cdef extern from "<stdint.h>":
    ctypedef unsigned long uintptr_t

# Define sizes for the Bloom filters
cdef uint32_t BYTE_ARRAY_SIZE_TINY = 1 * 1024          # 1 KB for <= 1K records
cdef uint32_t BYTE_ARRAY_SIZE_SMALL = 64 * 1024        # 64 KB for <= 60K records
cdef uint32_t BYTE_ARRAY_SIZE_LARGE = 1024 * 1024      # 1 MB for <=  1M records
cdef uint32_t BYTE_ARRAY_SIZE_HUGE = 16 * 1024 * 1024  # 16 MB for <= 16M records

cdef uint32_t BIT_ARRAY_SIZE_TINY = BYTE_ARRAY_SIZE_TINY << 3    # 8 Kbits
cdef uint32_t BIT_ARRAY_SIZE_SMALL = BYTE_ARRAY_SIZE_SMALL << 3  # 512 Kbits
cdef uint32_t BIT_ARRAY_SIZE_LARGE = BYTE_ARRAY_SIZE_LARGE << 3  # 8 Mbits
cdef uint32_t BIT_ARRAY_SIZE_HUGE = BYTE_ARRAY_SIZE_HUGE << 3    # 128 Mbits


cdef class BloomFilter:
    # defined in the .pxd file only - here so they aren't magic
    # cdef unsigned char* bit_array
    # cdef uint32_t bit_array_size
    # cdef uint32_t byte_array_size

    def __cinit__(self, uint32_t expected_records=50000):
        """Initialize Bloom Filter based on expected number of records."""
        if expected_records <= 1_000:
            self.byte_array_size = BYTE_ARRAY_SIZE_TINY
            self.bit_array_size = BIT_ARRAY_SIZE_TINY
        elif expected_records <= 62_000:
            self.byte_array_size = BYTE_ARRAY_SIZE_SMALL
            self.bit_array_size = BIT_ARRAY_SIZE_SMALL
        elif expected_records <= 1_000_000:
            self.byte_array_size = BYTE_ARRAY_SIZE_LARGE
            self.bit_array_size = BIT_ARRAY_SIZE_LARGE
        elif expected_records <= 16_000_000:
            self.byte_array_size = BYTE_ARRAY_SIZE_HUGE
            self.bit_array_size = BIT_ARRAY_SIZE_HUGE
        else:
            raise ValueError("Too many records for this Bloom filter implementation")

        # Allocate memory
        self.bit_array = <unsigned char*>calloc(self.byte_array_size, sizeof(uint8_t))
        if not self.bit_array:
            raise MemoryError("Failed to allocate memory for the Bloom filter.")

    def __dealloc__(self):
        if self.bit_array:
            free(self.bit_array)

    cdef inline void _add(self, const uint64_t item):
        cdef uint32_t h1, h2

        h1 = item & (self.bit_array_size - 1)
        # Apply the golden ratio to the item and use a mask to keep within the
        # size of the bit array.
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        self.bit_array[h1 >> 3] |= 1 << (h1 & 7)
        self.bit_array[h2 >> 3] |= 1 << (h2 & 7)

    cpdef void add(self, const uint64_t item):
        self._add(item)

    cdef inline bint _possibly_contains(self, const uint64_t item):
        """Check if the item might be in the set"""
        cdef uint32_t h1, h2

        h1 = item & (self.bit_array_size - 1)
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        return (((self.bit_array[h1 >> 3] >> (h1 & 7)) & 1) != 0) and \
               (((self.bit_array[h2 >> 3] >> (h2 & 7)) & 1) != 0)

    cpdef bint possibly_contains(self, const uint64_t item):
        return self._possibly_contains(item)

    cpdef numpy.ndarray[numpy.npy_bool, ndim=1] possibly_contains_many(self, object relation, list columns):
        """
        Return a boolean array indicating whether each row in `relation` might be in the Bloom filter.
        Null-containing rows are considered not present (False).
        """
        cdef:
            Py_ssize_t num_rows = relation.num_rows
            numpy.ndarray[numpy.npy_bool, ndim=1] result = numpy.zeros(num_rows, dtype=numpy.bool)
            uint8_t[::1] result_view = result
            int64_t[::1] valid_row_ids = non_null_row_indices(relation, columns)
            Py_ssize_t num_valid_rows = valid_row_ids.shape[0]
            numpy.ndarray[numpy.uint64_t, ndim=1] row_hashes_np = numpy.zeros(num_rows, dtype=numpy.uint64)
            uint64_t[::1] row_hashes = row_hashes_np
            Py_ssize_t i
            int64_t row_id

        if num_valid_rows == 0:
            return result

        # Compute hashes only for non-null rows
        compute_row_hashes(relation, columns, row_hashes)

        for i in range(num_valid_rows):
            row_id = valid_row_ids[i]
            result_view[row_id] = self._possibly_contains(row_hashes[row_id])

        return result

cpdef BloomFilter create_bloom_filter(object relation, list columns):
    """
    Create a BloomFilter from the specified `columns` in `relation`,
    ignoring rows with nulls in any of the columns.
    """
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        int64_t[::1] valid_row_ids = non_null_row_indices(relation, columns)
        Py_ssize_t num_valid_rows = valid_row_ids.shape[0]
        numpy.ndarray[numpy.uint64_t, ndim=1] row_hashes_np = numpy.empty(num_rows, dtype=numpy.uint64)
        uint64_t[::1] row_hashes = row_hashes_np
        Py_ssize_t i
        BloomFilter bf = BloomFilter(num_valid_rows)

    if num_valid_rows == 0:
        return bf

    # Populate row hashes using the selected columns
    compute_row_hashes(relation, columns, row_hashes)

    # Add to bloom filter
    for i in range(num_valid_rows):
        bf._add(row_hashes[valid_row_ids[i]])

    return bf
