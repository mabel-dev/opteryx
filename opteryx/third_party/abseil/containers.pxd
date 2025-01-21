# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: lintrule=ignore

from libc.stdint cimport int64_t
from libcpp.pair cimport pair
from libcpp.vector cimport vector


cdef extern from "absl/container/flat_hash_map.h" namespace "absl":
    cdef cppclass flat_hash_map[K, V]:
        flat_hash_map()
        V& operator[](K key)
        size_t size() const
        void clear()

cdef class FlatHashMap:
    cdef flat_hash_map[int64_t, vector[int64_t]] _map

    cpdef insert(self, int64_t key, int64_t value)
    cpdef size_t size(self)
    cpdef clear(self)
    cpdef vector[int64_t] get(self, int64_t key)

cdef extern from "absl/container/flat_hash_set.h" namespace "absl":
    cdef cppclass flat_hash_set[T]:
        flat_hash_set()
        pair[long, bint] insert(T value)
        size_t size() const
        bint contains(T value) const
        void reserve(int64_t value)

cdef class FlatHashSet:
    cdef flat_hash_set[int64_t] _set

    cdef inline bint insert(self, int64_t value)
    cdef inline size_t size(self)
    cdef inline bint contains(self, int64_t value)
    cpdef size_t items(self)