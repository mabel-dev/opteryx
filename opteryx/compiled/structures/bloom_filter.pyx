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
    - A 512k slot bit array for up to 50k items (about 3% FPR)
    - a 8m slot bit array for up to 1m items (about 2% FPR)

We perform one hash and then use a calculation based on the golden ratio to
determine the second position. This is cheaper than performing two hashes whilst
still providing a good enough split of the two hashes.

The primary use for this structure is to prefilter JOINs, it is many times faster
(about 20x from initial benchmarking) to test for containment in the bloom filter
that to look up the item in the hash table.

Building the filter is fast - for tables up to 1 million records we create the filter
(1m records is roughly a 0.07s build). If the filter isn't effective (less that 5%
eliminations) we discard it which has meant some waste work.
"""

from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy
from libc.stdint cimport uint8_t, int64_t

from opteryx.third_party.cyan4973.xxhash cimport cy_xxhash3_64

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
    # defined in the .pxd file only - here so they aren't magic
    # cdef unsigned char* bit_array
    # cdef uint32_t bit_array_size
    # cdef uint32_t byte_array_size

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

    cdef inline void _add(self, bytes member):
        cdef uint32_t item, h1, h2

        item = cy_xxhash3_64(<char*>member, len(member))
        h1 = item & (self.bit_array_size - 1)
        # Apply the golden ratio to the item and use a mask to keep within the
        # size of the bit array.
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        set_bit(self.bit_array, h1)
        set_bit(self.bit_array, h2)

    cpdef void add(self, bytes member):
        self._add(member)

    cdef inline bint _possibly_contains(self, bytes member):
        """Check if the item might be in the set"""
        cdef uint32_t item, h1, h2

        item = cy_xxhash3_64(<char*>member, len(member))
        h1 = item & (self.bit_array_size - 1)
        h2 = (item * 2654435769U) & (self.bit_array_size - 1)
        return ((self.bit_array[h1 >> 3] & (1 << (h1 & 7))) != 0) and \
               ((self.bit_array[h2 >> 3] & (1 << (h2 & 7))) != 0)

    cpdef bint possibly_contains(self, bytes member):
        return self._possibly_contains(member)

    cpdef cnp.ndarray[cnp.npy_bool, ndim=1] possibly_contains_many(self, cnp.ndarray keys):
        """
        Return a boolean array indicating whether each key might be in the Bloom filter.

        Parameters:
            keys: cnp.ndarray
                Array of keys to test for membership.

        Returns:
            A boolean array of the same length as `keys` with True or False values.
        """
        cdef Py_ssize_t i
        cdef Py_ssize_t n = keys.shape[0]

        # Create an uninitialized bool array rather than a zeroed one
        cdef cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.empty(n, dtype=numpy.bool_)

        # Wrap both `keys` and `result` in typed memory views for faster indexing
        cdef object[::1] keys_view = keys
        cdef uint8_t[::1] result_view = result

        for i in range(n):
            result_view[i] = False if keys_view[i] is None else self._possibly_contains(keys_view[i])

        return result

    cpdef cnp.ndarray[cnp.npy_bool, ndim=1] possibly_contains_many_ints(self, cnp.ndarray[cnp.int64_t] keys):
        """
        Return a boolean array indicating whether each key might be in the Bloom filter.

        Parameters:
            keys: cnp.ndarray
                Array of keys to test for membership.

        Returns:
            A boolean array of the same length as `keys` with True or False values.
        """
        cdef Py_ssize_t i
        cdef Py_ssize_t n = keys.shape[0]
        cdef Py_ssize_t bit_array_size = self.bit_array_size

        # Create an uninitialized bool array rather than a zeroed one
        cdef cnp.ndarray[cnp.npy_bool, ndim=1] result = numpy.empty(n, dtype=numpy.bool_)

        # Wrap both `keys` and `result` in typed memory views for faster indexing
        cdef uint8_t[::1] result_view = result

        for i in range(n):
            h1 = (keys[i] * 2654435769U) & (bit_array_size - 1)
            result_view[i] = ((self.bit_array[h1 >> 3] & (1 << (h1 & 7))) != 0)

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

    cdef Py_ssize_t n = len(keys)
    cdef Py_ssize_t i
    cdef BloomFilter bf = BloomFilter(n)

    keys = keys.drop_null()
    keys = keys.cast(pyarrow.binary()).to_numpy(False)

    cdef object[::1] keys_view = keys  # Memory view for fast access

    for i in range(len(keys)):
        bf._add(keys_view[i])

    return bf


cpdef BloomFilter create_int_bloom_filter(cnp.ndarray[cnp.int64_t] keys):

    cdef Py_ssize_t n = len(keys)
    cdef Py_ssize_t i
    cdef BloomFilter bf = BloomFilter(n)
    cdef int64_t h1

    cdef Py_ssize_t bit_array_size = bf.bit_array_size

    for i in range(len(keys)):
        h1 = (keys[i] * 2654435769U) & (bit_array_size - 1)
        set_bit(bf.bit_array, h1)

    return bf
