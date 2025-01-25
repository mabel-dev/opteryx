# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libcpp.vector cimport vector
from libc.stdint cimport int64_t
from libcpp.pair cimport pair


cdef extern from "absl/container/flat_hash_map.h" namespace "absl":
    cdef cppclass flat_hash_map[K, V, HashFunc]:
        flat_hash_map()
        V& operator[](K key)
        size_t size() const
        void clear()

cdef class FlatHashMap:
    #cdef flat_hash_map[int64_t, vector[int64_t]] _map

    def __cinit__(self):
        self._map = flat_hash_map[int64_t, vector[int64_t], IdentityHash]()

    cpdef insert(self, key: int64_t, value: int64_t):
        self._map[key].push_back(value)

    cpdef size_t size(self):
        return self._map.size()

    cpdef clear(self):
        self._map.clear()

    cpdef vector[int64_t] get(self, int64_t key):
        return self._map[key]

cdef extern from "absl/container/flat_hash_set.h" namespace "absl":
    cdef cppclass flat_hash_set[T, HashFunc]:
        flat_hash_set()
        pair[long, bint] insert(T value)
        size_t size() const
        bint contains(T value) const
        void reserve(int64_t value)

cdef class FlatHashSet:
    #cdef flat_hash_set[int64_t, IdentityHash] _set

    def __cinit__(self):
        self._set = flat_hash_set[int64_t, IdentityHash]()
        self._set.reserve(256)

    cdef inline bint insert(self, value: int64_t):
        return self._set.insert(value).second

    cdef inline size_t size(self):
        return self._set.size()

    cdef inline bint contains(self, int64_t value):
        return self._set.contains(value)

    cpdef size_t items(self):
        return self._set.size()
