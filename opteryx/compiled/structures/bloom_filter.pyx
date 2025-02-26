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

We have two size options, both using 2 hashes:
    - a 512k slot bit array for up to 60k items (about 4.2% FPR)
    - a 8m slot bit array for up to 1m items (about 4.5% FPR)
    - a 128m slot but array for up to 16m items (about 4.7% FPR)

We perform one hash and then use a calculation based on the golden ratio to
determine the second position. This is cheaper than performing two hashes whilst
still providing a good enough split of the two hashes.

The primary use for this structure is to prefilter JOINs, it is many times faster
(about 20x from initial benchmarking) to test for containment in the bloom filter
that to look up the item in the hash table.

Building the filter is fast - for tables up to 1 million records we create the filter
(1m records is roughly a 0.01s build). If the filter isn't effective (less that 5%
eliminations) we discard it which has meant some waste work.

The 16m set is the limit at the moment, it takes about 0.23 seconds to build which
is the limit of what we think we should speculatively build.
"""

from libc.stdlib cimport calloc, free
from libc.stdint cimport uint8_t, int32_t

from opteryx.third_party.cyan4973.xxhash cimport cy_xxhash3_64

import numpy
cimport numpy
import pyarrow

cdef extern from "<stdint.h>":
    ctypedef unsigned long uintptr_t

# Define sizes for the two Bloom filters
cdef uint32_t BYTE_ARRAY_SIZE_SMALL = 64 * 1024        # 64 KB for <= 60K records
cdef uint32_t BYTE_ARRAY_SIZE_LARGE = 1024 * 1024      # 1 MB for <=  1M records
cdef uint32_t BYTE_ARRAY_SIZE_HUGE = 16 * 1024 * 1024  # 8 MB for <= 16M records

cdef uint32_t BIT_ARRAY_SIZE_SMALL = BYTE_ARRAY_SIZE_SMALL << 3  # 512 Kbits
cdef uint32_t BIT_ARRAY_SIZE_LARGE = BYTE_ARRAY_SIZE_LARGE << 3  # 8 Mbits
cdef uint32_t BIT_ARRAY_SIZE_HUGE = BYTE_ARRAY_SIZE_HUGE << 3    # 128 Mbits

cdef uint8_t bit_masks[8]
bit_masks[0] = 1
bit_masks[1] = 2
bit_masks[2] = 4
bit_masks[3] = 8
bit_masks[4] = 16
bit_masks[5] = 32
bit_masks[6] = 64
bit_masks[7] = 128

cdef int64_t EMPTY_HASH = <int64_t>0xBADC0FFEE

cdef class BloomFilter:
    # defined in the .pxd file only - here so they aren't magic
    # cdef uint8_t* bit_array_backing
    # cdef unsigned char* bit_array
    # cdef uint32_t bit_array_size
    # cdef uint32_t byte_array_size

    def __cinit__(self, uint32_t expected_records=50000):
        """Initialize Bloom Filter based on expected number of records."""
        if expected_records <= 62_000:
            self.byte_array_size = BYTE_ARRAY_SIZE_SMALL
            self.bit_array_size = BIT_ARRAY_SIZE_SMALL
        elif expected_records <= 1_000_001:
            self.byte_array_size = BYTE_ARRAY_SIZE_LARGE
            self.bit_array_size = BIT_ARRAY_SIZE_LARGE
        elif expected_records <= 16_000_001:
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

    cdef inline void _add(self, const void *member, size_t length):
        cdef uint32_t item, h1, h2

        item = cy_xxhash3_64(member, length)
        h1 = item & (self.bit_array_size - 1)
        # Apply the golden ratio to the item and use a mask to keep within the
        # size of the bit array.
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        self.bit_array[h1 >> 3] |= bit_masks[h1 & 7]
        self.bit_array[h2 >> 3] |= bit_masks[h2 & 7]

    cpdef void add(self, bytes member):
        self._add(<char*>member, len(member))

    cdef inline bint _possibly_contains(self, const void *member, size_t length):
        """Check if the item might be in the set"""
        cdef uint32_t item, h1, h2

        item = cy_xxhash3_64(member, length)
        h1 = item & (self.bit_array_size - 1)
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        return ((self.bit_array[h1 >> 3] & bit_masks[h1 & 7]) != 0) and \
               ((self.bit_array[h2 >> 3] & bit_masks[h2 & 7]) != 0)

    cpdef bint possibly_contains(self, bytes member):
        return self._possibly_contains(<char*>member, len(member))

    cpdef numpy.ndarray[numpy.npy_bool, ndim=1] possibly_contains_many(self, keys):
        """
        Return a boolean array indicating whether each key might be in the Bloom filter.

        Parameters:
            keys: numpy.ndarray
                Array of keys to test for membership.

        Returns:
            A boolean array of the same length as `keys` with True or False values.
        """
        cdef Py_ssize_t i, j, length
        cdef const char* data
        cdef const uint8_t* validity
        cdef const int32_t* offsets
        cdef Py_ssize_t arr_offset, offset_in_bits, offset_in_bytes
        cdef Py_ssize_t start_offset, end_offset
        cdef const char* empty_str = b""

        # Create BloomFilter
        cdef Py_ssize_t n = len(keys)
        cdef result = numpy.empty(n, dtype=numpy.bool)
        cdef uint8_t[::1] result_view = result

        i = 0
        for chunk in keys.chunks if isinstance(keys, pyarrow.ChunkedArray) else [keys]:
            buffers = chunk.buffers()
            offsets = <const int32_t*><uintptr_t>buffers[1].address
            data = <const char*><uintptr_t>buffers[2].address
            validity = <const uint8_t*><uintptr_t>buffers[0].address if buffers[0] else NULL
            length = len(chunk)
            arr_offset = chunk.offset

            # Calculate the byte and bit offset for validity
            offset_in_bits = arr_offset & 7
            offset_in_bytes = arr_offset >> 3

            for j in range(length):
                result_view[i] = 0
                byte_index = offset_in_bytes + ((offset_in_bits + j) >> 3)
                bit_index = (offset_in_bits + j) & 7
                if validity == NULL or (validity[byte_index] & (1 << bit_index)):
                    start_offset = offsets[arr_offset + j]
                    end_offset = offsets[arr_offset + j + 1]
                    if start_offset >= 0 and end_offset >= start_offset:
                        if end_offset > start_offset:
                            result_view[i] = self._possibly_contains(
                                data + start_offset,
                                end_offset - start_offset
                            )
                        else:
                            result_view[i] = self._possibly_contains(empty_str, 0)
                i += 1

        return result

cpdef BloomFilter create_bloom_filter(keys):
    cdef Py_ssize_t j, length
    cdef const char* data
    cdef const uint8_t* validity
    cdef const int32_t* offsets
    cdef Py_ssize_t start_offset, end_offset, arr_offset, offset_in_bits, offset_in_bytes
    cdef const char* empty_str = b""

    cdef Py_ssize_t n = len(keys)
    cdef BloomFilter bf = BloomFilter(n)

    for chunk in keys.chunks if isinstance(keys, pyarrow.ChunkedArray) else [keys]:
        buffers = chunk.buffers()
        offsets = <const int32_t*><uintptr_t>buffers[1].address
        data = <const char*><uintptr_t>buffers[2].address
        validity = <const uint8_t*><uintptr_t>buffers[0].address if buffers[0] else NULL
        length = len(chunk)
        arr_offset = chunk.offset

        offset_in_bits = arr_offset & 7
        offset_in_bytes = arr_offset >> 3

        for j in range(length):

            # locate validity bit for this row
            byte_index = offset_in_bytes + ((offset_in_bits + j) >> 3)
            bit_index = (offset_in_bits + j) & 7

            if validity == NULL or (validity[byte_index] & (1 << bit_index)):
                # Use chunk-local offsets
                start_offset = offsets[arr_offset + j]
                end_offset = offsets[arr_offset + j + 1]
                if start_offset >= 0 and end_offset >= start_offset:
                    if end_offset > start_offset:
                        bf._add(data + start_offset, end_offset - start_offset)
                    else:
                        bf._add(empty_str, 0)  # Normalize empty strings

    return bf
