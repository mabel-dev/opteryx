# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Optimized LRU-K(2) implementation focused on performance.

Key optimizations:
1. Simplified data structures - removed unnecessary heap and tracking
2. Direct dictionary access with minimal indirection
3. Simple list-based access history (faster than deque for our use case)
4. Reduced memory allocations and copies
5. Minimal bookkeeping overhead
"""

from collections import OrderedDict
from libc.stdint cimport int64_t

cdef class LRU_K:
    __slots__ = ("k", "max_size", "max_memory", "current_memory", "slots",
                 "access_history", "_clock", "hits", "misses",
                 "evictions", "inserts", "size")

    cdef public int64_t k
    cdef public int64_t max_size
    cdef public int64_t max_memory
    cdef int64_t current_memory
    cdef object slots
    cdef dict access_history
    cdef int64_t _clock
    cdef int64_t hits
    cdef int64_t misses
    cdef int64_t evictions
    cdef int64_t inserts
    cdef public int64_t size

    def __cinit__(self, int64_t k=2, int64_t max_size=0, int64_t max_memory=0):
        """
        Initialize LRU-K cache.

        Args:
            k: K value for LRU-K algorithm
            max_size: Maximum number of items (0 for unlimited)
            max_memory: Maximum memory in bytes (0 for unlimited)
        """
        if k < 1:
            raise ValueError("k must be at least 1")
        self.k = k
        self.max_size = max_size
        self.max_memory = max_memory
        self.current_memory = 0
        self.slots = OrderedDict()
        self.access_history = {}
        self._clock = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.inserts = 0
        self.size = 0

    def __len__(self):
        return self.size

    def __contains__(self, bytes key):
        return key in self.slots

    cpdef object get(self, bytes key):
        """Get value for key, updating access history. Returns bytes or None."""
        cdef object value = self.slots.get(key)
        if value is not None:
            self.hits += 1
            self._update_access_history(key)
            # Move to end to maintain LRU order
            self.slots.move_to_end(key)
        else:
            self.misses += 1
        return value

    cpdef tuple set(self, bytes key, bytes value, bint evict=True):
        """
        Set key-value pair, optionally evicting if needed.

        Returns:
            Evicted key-value pair if eviction occurred, else None
        """
        cdef bytes evicted_key = None
        cdef bytes evicted_value = None
        cdef bint key_exists = key in self.slots

        self.inserts += 1

        # Calculate memory impact
        cdef int64_t item_memory = len(key) + len(value)

        if not key_exists:
            self.size += 1
            self.current_memory += item_memory
        else:
            # Update memory usage for existing key
            old_value = self.slots[key]
            self.current_memory += len(value) - len(old_value)

        # Insert/update the slot value. Do NOT update access history on set();
        # access history should reflect actual accesses (gets) only. This keeps
        # behaviour consistent with LRU-K expectations and existing tests.
        self.slots[key] = value
        # Update access history on set as well (insertion counts as an access)
        # This mirrors the previous behavior and keeps eviction semantics stable.
        self._update_access_history(key)

        # Move to end to maintain LRU order
        self.slots.move_to_end(key)

        # Evict if needed and requested
        if evict:
            evicted_key, evicted_value = self._evict_if_needed()

        return evicted_key, evicted_value

    cdef void _update_access_history(self, bytes key):
        """Update access history for key using a simple list (faster than deque)."""
        cdef int64_t access_time
        cdef list history

        # Increment clock
        self._clock += 1
        access_time = self._clock

        history = self.access_history.get(key)
        if history is None:
            history = [access_time]
            self.access_history[key] = history
        else:
            history.append(access_time)
            # Keep only the last k entries
            if len(history) > self.k:
                history.pop(0)

    cdef tuple _evict_if_needed(self):
        """Evict items if size or memory limits are exceeded."""
        cdef bytes evicted_key = None
        cdef bytes evicted_value = None

        while self._should_evict():
            evicted_key, evicted_value = self._evict_one()
            if evicted_key is None:
                break  # No more items to evict

        return evicted_key, evicted_value

    cdef bint _should_evict(self):
        """Check if eviction is needed."""
        if self.max_size > 0 and self.size > self.max_size:
            return True
        if self.max_memory > 0 and self.current_memory > self.max_memory:
            return True
        return False

    cpdef object evict(self, bint details=False):
        """Evict one item according to LRU-K policy.

        If details is False (default) return the evicted key or None.
        If details is True return a (key, value) tuple or (None, None).
        """
        cdef tuple result = self._evict_one(details)
        if details:
            return result
        # return only the key when details is False
        return result[0]

    cdef tuple _evict_one(self, bint details=False):
        """Evict one item using simplified LRU-K algorithm."""
        cdef bytes candidate_key = None
        cdef bytes candidate_value = None
        cdef int64_t kth_time
        cdef list history

        if not self.slots:
            if details:
                return None, None
            return None, None

        # Find the key with the oldest kth access time.
        # First prefer keys that have at least k accesses (full history). If
        # none exist, fall back to keys with fewer than k accesses. This
        # enforces LRU-K: items with insufficient access history are evicted
        # only as a last resort.
        cdef int found_full_history = 0
        cdef int64_t candidate_time = -1

        # First pass: look for keys with >= k accesses
        for key in self.slots:
            history = self.access_history.get(key)
            if history is None:
                continue
            if len(history) >= self.k:
                kth_time = history[0]
                if candidate_key is None or kth_time < candidate_time:
                    candidate_key = key
                    candidate_time = kth_time
                    found_full_history = 1

        if not found_full_history:
            # Second pass: consider keys with partial history (len < k)
            for key in self.slots:
                history = self.access_history.get(key)
                if history is None:
                    # No history means never accessed; consider as last fallback
                    if candidate_key is None:
                        candidate_key = key
                    break
                # use first access time
                kth_time = history[0]
                if candidate_key is None or kth_time < candidate_time:
                    candidate_key = key
                    candidate_time = kth_time

        if candidate_key is None:
            if details:
                return None, None
            return None, None

        # Remove the candidate
        candidate_value = self.slots.pop(candidate_key, None)

        if candidate_key in self.access_history:
            del self.access_history[candidate_key]

        self.size -= 1
        if candidate_value is not None:
            self.current_memory -= (len(candidate_key) + len(candidate_value))
        self.evictions += 1

        if details:
            return candidate_key, candidate_value
        return candidate_key, None

    cpdef bint delete(self, bytes key):
        """Delete specific key from cache."""
        if key in self.slots:
            value = self.slots.pop(key)
            if key in self.access_history:
                del self.access_history[key]
            self.size -= 1
            self.current_memory -= (len(key) + len(value))
            self.evictions += 1
            return True
        return False

    cpdef void clear(self, bint reset_stats=False):
        """Clear all items from cache."""
        self.slots.clear()
        self.access_history.clear()
        self.size = 0
        self.current_memory = 0
        if reset_stats:
            self.hits = 0
            self.misses = 0
            self.evictions = 0
            self.inserts = 0

    @property
    def keys(self):
        """Get all keys in cache as a list."""
        return list(self.slots.keys())

    def items(self):
        """Get all key-value pairs in cache."""
        return list(self.slots.items())

    @property
    def memory_usage(self):
        """Get current memory usage in bytes."""
        return self.current_memory

    @property
    def stats(self):
        """Get cache statistics as a tuple: (hits, misses, evictions, inserts)."""
        return (self.hits, self.misses, self.evictions, self.inserts)

    def reset(self, bint reset_stats=False):
        """Alias for clear."""
        self.clear(reset_stats)
