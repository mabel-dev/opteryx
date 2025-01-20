# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
This is not a general perpose Bloom Filter, if used outside of Draken, it may not
perform entirely as expected as it is optimized for a specific configuration.

We have two size options, both using 2 hashes:
    - A 512k slot bit array for up to 50k items (about 3% FPR)
    - a 8m slot bit array for up to 1m items (about 2% FPR)

We perform one hash and then use a calculation based on the golden ratio to
determine the second position.
"""

from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy

from opteryx.compiled.functions.murmurhash3_32 cimport cy_murmurhash3

import numpy
cimport numpy as cnp
import pyarrow

# Define sizes for the two Bloom filters
cdef uint32_t BYTE_ARRAY_SIZE_SMALL = 64 * 1024    # 64 KB for ~50K records
cdef uint32_t BYTE_ARRAY_SIZE_LARGE = 1024 * 1024  # 1 MB for ~1M records

cdef uint32_t BIT_ARRAY_SIZE_SMALL = BYTE_ARRAY_SIZE_SMALL << 3  # 512 Kbits
cdef uint32_t BIT_ARRAY_SIZE_LARGE = BYTE_ARRAY_SIZE_LARGE << 3  # 8 Mbits


cdef inline void set_bit(unsigned char* bit_array, uint32_t bit):
    cdef uint32_t byte_idx = bit >> 3
    cdef uint32_t bit_idx = bit & 7
    bit_array[byte_idx] |= 1 << bit_idx

cdef class BloomFilter:
    #    cdef unsigned char* bit_array  # defined in the .pxd file only
    #    cdef uint32_t bit_array_size
    #    cdef uint32_t byte_array_size

    def __cinit__(self, uint32_t expected_records=50000):
        """Initialize Bloom Filter based on expected number of records."""
        if expected_records <= 50000:
            self.byte_array_size = BYTE_ARRAY_SIZE_SMALL
            self.bit_array_size = BIT_ARRAY_SIZE_SMALL
        elif expected_records <= 1000000:
            self.byte_array_size = BYTE_ARRAY_SIZE_LARGE
            self.bit_array_size = BIT_ARRAY_SIZE_LARGE
        else:
            raise ValueError("Too many records for this Bloom filter implementation")

        # Allocate memory
        self.bit_array = <unsigned char*>malloc(self.byte_array_size)
        if not self.bit_array:
            raise MemoryError("Failed to allocate memory for the Bloom filter.")
        memset(self.bit_array, 0, self.byte_array_size)

    def __dealloc__(self):
        if self.bit_array:
            free(self.bit_array)

    cpdef void add(self, bytes member):
        cdef uint32_t item, h1, h2

        item = cy_murmurhash3(<char*>member, len(member), 0)
        h1 = item & (self.bit_array_size - 1)
        # Apply the golden ratio to the item and use modulo to wrap within the size of the bit array
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        # Set bits
        set_bit(self.bit_array, h1)
        set_bit(self.bit_array, h2)

    cdef inline bint _possibly_contains(self, bytes member):
        """Check if the item might be in the set"""
        cdef uint32_t item, h1, h2

        item = cy_murmurhash3(<char*>member, len(member), 0)
        h1 = item & (self.bit_array_size - 1)
        # Apply the golden ratio to the item and mask within the size of the bit array
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        # Check bits using bitwise AND
        return ((self.bit_array[h1 >> 3] & (1 << (h1 & 7))) != 0) and \
               ((self.bit_array[h2 >> 3] & (1 << (h2 & 7))) != 0)

    cpdef bint possibly_contains(self, bytes member):
        return self._possibly_contains(member)

    cpdef cnp.ndarray[cnp.npy_bool, ndim=1] possibly_contains_many(self, cnp.ndarray keys):
        cdef Py_ssize_t i
        cdef Py_ssize_t n = len(keys)
        cdef cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.zeros(n, dtype=bool)

        for i in range(n):
            key = keys[i]
            if key is not None and self._possibly_contains(key):
                result[i] = 1
        return result

    cpdef memoryview serialize(self):
        """Serialize the Bloom filter to a memory view"""
        return memoryview(self.bit_array[:self.byte_array_size])

cpdef BloomFilter deserialize(const unsigned char* data):
    """Deserialize a memory view to a Bloom filter"""
    cdef BloomFilter bf = BloomFilter()
    memcpy(bf.bit_array, data, bf.byte_array_size)
    return bf


cpdef BloomFilter create_bloom_filter(keys):
    cdef BloomFilter bf = BloomFilter(len(keys))

    keys = keys.drop_null()
    keys = keys.cast(pyarrow.binary()).to_numpy(False)
    for key in keys:
        bf.add(key)

    return bf
