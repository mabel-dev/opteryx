# cython: language_level=3

"""
approximately
- 500 items, with two hashes, in 4092 bits, would have a 5% FP rate.
This implementation runs in about 1/3 the time of the one in Orso
"""


from libc.stdlib cimport malloc, free
from libc.string cimport memset

cdef class BloomFilter:
    cdef:
        unsigned char* bit_array
        long size

    def __cinit__(self, long size):
        self.size = size
        # Allocate memory for the bit array and initialize to 0
        self.bit_array = <unsigned char*>malloc(size // 8 + 1)
        if not self.bit_array:
            raise MemoryError("Failed to allocate memory for the bit array.")
        memset(self.bit_array, 0, size // 8 + 1)

    def __dealloc__(self):
        if self.bit_array:
            free(self.bit_array)

    cpdef void add(self, long item):
        """Add an item to the Bloom filter"""
        h1 = item % self.size
        # Apply the golden ratio to the item and use modulo to wrap within the size of the bit array
        h2 = <long>(item * 1.618033988749895) % self.size
        # Set bits using bitwise OR
        self.bit_array[h1 // 8] |= 1 << (h1 % 8)
        self.bit_array[h2 // 8] |= 1 << (h2 % 8)

    cpdef int possibly_contains(self, long item):
        """Check if the item might be in the set"""
        h1 = item % self.size
        h2 = (item * item + 1) % self.size
        # Check bits using bitwise AND
        return (self.bit_array[h1 // 8] & (1 << (h1 % 8))) and \
               (self.bit_array[h2 // 8] & (1 << (h2 % 8)))

def create_bloom_filter(int size, items):
    """Create and populate a Bloom filter"""
    cdef BloomFilter bf = BloomFilter(size)
    for item in items:
        bf.add(item)
    return bf
