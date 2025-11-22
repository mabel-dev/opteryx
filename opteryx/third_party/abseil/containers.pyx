# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uint64_t, int32_t
from libc.stddef cimport size_t
from libcpp.pair cimport pair


cdef extern from "absl/container/flat_hash_map.h" namespace "absl":
    cdef cppclass flat_hash_map[K, V, HashFunc]:
        flat_hash_map()
        V& operator[](K key)
        size_t size() const
        void clear()

cdef class FlatHashMap:
    #cdef flat_hash_map[uint64_t, vector[int64_t]] _map

    def __cinit__(self):
        self._map = flat_hash_map[uint64_t, vector[int64_t], IdentityHash]()

    cpdef insert(self, key: uint64_t, value: int64_t):
        self._map[key].push_back(value)

    cpdef size_t size(self):
        return self._map.size()

    cpdef clear(self):
        self._map.clear()

    cpdef vector[int64_t] get(self, uint64_t key):
        return self._map[key]

cdef extern from "absl/container/flat_hash_set.h" namespace "absl":
    cdef cppclass flat_hash_set[T, HashFunc]:
        flat_hash_set()
        pair[long, bint] insert(T value)
        size_t size() const
        bint contains(T value) const
        void reserve(int64_t value)

cdef class FlatHashSet:
    #cdef flat_hash_set[uint64_t, IdentityHash] _set

    def __cinit__(self):
        self._set = flat_hash_set[uint64_t, IdentityHash]()
        self._set.reserve(1024)

    cdef inline bint insert(self, value: uint64_t) noexcept nogil:
        return self._set.insert(value).second

    cdef inline void just_insert(self, value: uint64_t) noexcept nogil:
        self._set.insert(value)

    cdef inline void insert_many(self, uint64_t* values, Py_ssize_t length) noexcept nogil:
        if values == NULL or length <= 0:
            return
        flat_hash_set_insert_many(self._set, <const uint64_t*>values, <size_t>length)

    cdef inline size_t size(self) noexcept nogil:
        return self._set.size()

    cdef inline bint contains(self, uint64_t value) noexcept nogil:
        return self._set.contains(value)

    cdef inline void reserve(self, int64_t capacity) noexcept nogil:
        self._set.reserve(capacity)

    cdef vector[int64_t] find_new_indices(self, uint64_t* hashes, Py_ssize_t length) noexcept nogil:
        cdef vector[int64_t] indices
        cdef Py_ssize_t i
        indices.reserve(length)  # Worst case

        for i in range(length):
            if self._set.insert(hashes[i]).second:
                indices.push_back(i)

        return indices

    cdef Py_ssize_t find_new_indices_out(self, uint64_t* hashes, Py_ssize_t length, int64_t* out_indices) noexcept nogil:
        cdef Py_ssize_t i
        cdef Py_ssize_t count = 0

        for i in range(length):
            if self._set.insert(hashes[i]).second:
                out_indices[count] = i
                count += 1

        return count

    cdef Py_ssize_t find_new_indices_out_32(self, uint64_t* hashes, Py_ssize_t length, int32_t* out_indices) noexcept nogil:
        cdef Py_ssize_t i
        cdef Py_ssize_t count = 0

        for i in range(length):
            if self._set.insert(hashes[i]).second:
                out_indices[count] = i
                count += 1

        return count

    cpdef size_t items(self):
        return self._set.size()
