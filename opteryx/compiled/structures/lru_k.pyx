# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
LRU-K evicts the morsel whose K-th most recent access is furthest in the past. Note, the
LRU doesn't evict, it has no "size", the caller decides when the cache is full, this
could be slot/count based (100 items), or this could be volume-based (32Mb).

This is a basic implementation of LRU-2, which evicts entries according to the time of their
penultimate access. The main benefit of this approach is to prevent a problem when the items being
getted exceeds the number of items in the cache. A classic LRU will evict and repopulate the cache
for every call. LRU-2 reduces the likelihood of this, but not preferring the MRU item to be
retained.

LRU-K should be used in conjunction with eviction limits per query - this appears to broadly be
the solution used by Postgres. This can be supported by the calling function using the return from
the .set call to determine if an item was evicted from the cache.

This can also be used as the index for an external cache (for example in plasma), where the set()
returns the evicted item which the calling function can then evict from the external cache.

This is a variation of LRU-K, where an item has fewer than K accesses, it is not evicted unless
all items have fewer than K accesses. Not being evicted adds an access to age out single-hit items
from the cache. The resulting cache provides opportunity for novel items to prove their value before
being evicted.

If n+1 items are put into the cache in the same 'transaction', it acts like a FIFO - although
the BufferPool implements limit to only evict up to 32 items per 'transaction'
"""

import heapq as py_heapq

from libc.stdint cimport int64_t
from collections import defaultdict
from time import monotonic_ns

cdef class LRU_K:

    __slots__ = ("k", "slots", "access_history", "removed", "heap",
                 "hits", "misses", "evictions", "inserts", "size")

    cdef public int64_t k
    cdef dict slots
    cdef object access_history
    cdef set removed
    cdef list heap

    cdef int64_t hits
    cdef int64_t misses
    cdef int64_t evictions
    cdef int64_t inserts
    cdef public int64_t size

    def __cinit__(self, int64_t k=2):
        self.k = k
        self.slots = {}
        self.access_history = defaultdict(list)
        self.removed = set()
        self.heap = []

        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.inserts = 0
        self.size = 0

    def __len__(self):
        return len(self.slots)

    def get(self, bytes key) -> Optional[bytes]:
        cdef object value = self.slots.get(key)
        if value is not None:
            self.hits += 1
            self._update_access_history(key)
        else:
            self.misses += 1
        return value

    def set(self, bytes key, bytes value):
        self.inserts += 1
        if key not in self.slots:
            self.size += 1
        self.slots[key] = value
        self._update_access_history(key)
        return None

    cdef void _update_access_history(self, bytes key):
        cdef int64_t access_time = monotonic_ns()
        cdef list history = self.access_history[key]
        if len(history) == self.k:
            old_entry = history.pop(0)
            self.removed.add(old_entry)
        history.append((access_time, key))
        py_heapq.heappush(self.heap, (access_time, key))

    def evict(self, bint details=False):
        cdef int64_t _oldest_access_time
        cdef bytes oldest_key
        cdef int64_t new_access_time
        cdef tuple popped
        while self.heap:
            popped = py_heapq.heappop(self.heap)
            _oldest_access_time, oldest_key = popped
            if popped in self.removed:
                self.removed.remove(popped)
                continue

            if len(self.access_history[oldest_key]) == 1:
                # Synthetic access to give a grace period
                new_access_time = monotonic_ns()
                self.access_history[oldest_key].append((new_access_time, oldest_key))
                py_heapq.heappush(self.heap, (new_access_time, oldest_key))
                continue

            if oldest_key not in self.slots:
                continue

            value = self.slots.pop(oldest_key)
            self.access_history.pop(oldest_key)
            self.size -= 1
            self.evictions += 1
            if details:
                return oldest_key, value
            return oldest_key

        if details:
            return None, None
        return None

    def delete(self, bytes key):
        if key in self.slots:
            self.slots.pop(key, None)
            self.access_history.pop(key, None)
            self.evictions += 1
            self.size -= 1
            return True
        return False

    @property
    def keys(self):
        return list(self.slots.keys())

    @property
    def stats(self):
        return self.hits, self.misses, self.evictions, self.inserts

    def reset(self, bint reset_stats=False):
        self.slots.clear()
        self.access_history.clear()
        self.removed.clear()
        self.heap.clear()
        if reset_stats:
            self.hits = 0
            self.misses = 0
            self.evictions = 0
            self.inserts = 0
