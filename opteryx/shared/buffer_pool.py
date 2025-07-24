# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Global Buffer Pool.

This is uses an LRU-K2 policy to determine what to keep and what to evict and is
backed by a MemoryPool.

The buffer pool has no slot limit, it is a given volume of memory, the pool
will try to evict when full. This is different to a classic Buffer Pool which
is slot-based.

The Buffer Pool is a global resource and used across all Connections and Cursors.
"""

from typing import Optional

from opteryx.compiled.structures.lru_k import LRU_K
from opteryx.config import MAX_LOCAL_BUFFER_CAPACITY
from opteryx.shared import MemoryPool


class _BufferPool:
    """
    Buffer Pool is a class implementing a Least Recently Used (LRU) policy for
    eviction.
    """

    slots = "_lru", "_memory_pool", "size"

    def __init__(self):
        self._lru = LRU_K(k=2)
        self._memory_pool = MemoryPool(name="BufferPool", size=MAX_LOCAL_BUFFER_CAPACITY)
        self.size = self._memory_pool.size

    def get(self, key: bytes, zero_copy: bool = True, latch: bool = True) -> Optional[bytes]:
        """
        Retrieve an item from the pool, return None if the item isn't found.
        """
        mp_key = self._lru.get(key)
        if mp_key is not None:
            mp_key = int.from_bytes(mp_key, "big")
            return self._memory_pool.read(mp_key, zero_copy=zero_copy, latch=latch)
        return None

    def unlatch(self, key: bytes):
        """
        Unlatch an item in the pool, allowing it to be moved if necessary.
        """
        mp_key = self._lru.get(key)
        if mp_key is not None:
            mp_key = int.from_bytes(mp_key, "big")
            self._memory_pool.unlatch(mp_key)

    def delete(self, key: bytes):
        """
        Delete an item from the pool.
        """
        mp_key = self._lru.get(key)
        if mp_key is not None:
            mp_key = int.from_bytes(mp_key, "big")
            self._memory_pool.release(mp_key)
            self._lru.delete(key)

    def set(self, key: bytes, value) -> Optional[str]:
        """
        Attempt to save a value to the buffer pool. Check first if there is space to commit the value.
        If not, evict the least recently used item and try again.

        Args:
            key: The key associated with the value to commit.
            value: The value to commit to the buffer pool.

        Returns:
            The key of the evicted item if eviction occurred, otherwise None.
        """
        # First check if we can commit the value to the memory pool
        if not self._memory_pool.available_space() >= len(value):
            evicted_key, evicted_value = self._lru.evict(details=True)
            if evicted_key:
                evicted_value = int.from_bytes(evicted_value, "big")
                self._memory_pool.release(evicted_value)
            else:
                return None  # Return None if no item could be evicted

        # Try to commit the value to the memory pool
        memory_pool_key = self._memory_pool.commit(value)
        if memory_pool_key == -1:
            return None  # Return None if commit still fails after eviction

        # Update LRU cache with the new key and memory pool key if commit succeeds
        self._lru.set(key, memory_pool_key.to_bytes(8, "big"))

        # Return the evicted key if an eviction occurred, otherwise return None
        return evicted_key if "evicted_key" in locals() else None

    @property
    def stats(self) -> tuple:
        """
        Return the hit, miss and eviction statistics for the buffer pool.
        """
        return self._lru.stats

    def reset(self, reset_stats: bool = False):
        """
        Reset the buffer pool.
        If reset_stats is True, also reset the statistics.
        """
        self._lru = LRU_K(k=2)
        self._memory_pool = MemoryPool(name="BufferPool", size=MAX_LOCAL_BUFFER_CAPACITY)
        self.size = self._memory_pool.size


class BufferPool(_BufferPool):
    """
    Singleton wrapper for the _BufferPool class. Only allows one instance of _BufferPool to exist.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = cls._create_instance()
        return cls._instance

    @classmethod
    def _create_instance(cls):
        return _BufferPool()

    @classmethod
    def reset(cls):
        """
        Reset the BufferPool singleton instance. This is useful when the configuration changes.
        """
        cls._instance = None
        cls._instance = cls._create_instance()
