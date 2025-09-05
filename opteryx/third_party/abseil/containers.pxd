# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: lintrule=ignore

from libc.stdint cimport int64_t, uint64_t
from libcpp.pair cimport pair
from libcpp.vector cimport vector

# Identity Hash Definition - not part of abseil but used by our implementation
# We prehash the values before putting them into the Map & Set, so don't rehash
cdef extern from *:
    """
    struct IdentityHash {
        inline size_t operator()(uint64_t value) const {
            return value;  // Identity function
        }
    };
    """
    cdef cppclass IdentityHash:
        size_t operator()(uint64_t value) const


cdef extern from "absl/container/flat_hash_map.h" namespace "absl":
    cdef cppclass flat_hash_map[K, V, HashFunc]:
        flat_hash_map()
        V& operator[](K key)
        size_t size() const
        void clear()

cdef class FlatHashMap:
    cdef flat_hash_map[uint64_t, vector[int64_t], IdentityHash] _map

    cpdef insert(self, uint64_t key, int64_t value)
    cpdef size_t size(self)
    cpdef clear(self)
    cpdef vector[int64_t] get(self, uint64_t key)

cdef extern from "absl/container/flat_hash_set.h" namespace "absl":
    cdef cppclass flat_hash_set[T, HashFunc]:
        flat_hash_set()
        pair[long, bint] insert(T value)
        size_t size() const
        bint contains(T value) const
        void reserve(int64_t value)

cdef class FlatHashSet:
    cdef flat_hash_set[uint64_t, IdentityHash] _set

    cdef inline bint insert(self, uint64_t value)
    cdef inline void just_insert(self, uint64_t value)
    cdef inline size_t size(self)
    cdef inline bint contains(self, uint64_t value)
    cpdef size_t items(self)