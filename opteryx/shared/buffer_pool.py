# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Global Buffer Pool.

This is uses an LRU-K2 policy to determine what to keep and what to evict and is
backed by a MemoryPool.

The buffer pool is has no slot limit, it is a given volume of memory, the pool
will try to evict when full. This is different to a classic Buffer Pool which
is slot-based.

The Buffer Pool is a global resource and used across all Connections and Cursors.
"""
from typing import Optional

from opteryx.shared.memory_pool import MemoryPool
from opteryx.utils.lru_2 import LRU2


class _BufferPool:
    """
    Buffer Pool is a class implementing a Least Recently Used (LRU) policy for
    eviction.
    """

    slots = "_lru", "_cache_backend", "_max_cacheable_item_size", "_memory_pool"

    def __init__(self):
        # Import here to avoid circular imports
        from opteryx import get_cache_manager

        cache_manager = get_cache_manager()

        self.max_cacheable_item_size = cache_manager.max_cacheable_item_size
        self._lru = LRU2()
        self._memory_pool = MemoryPool(
            name="BufferPool", size=cache_manager.max_local_buffer_capacity
        )

    def get(self, key: bytes) -> bytes:
        """
        Retrieve an item from the pool, return None if the item isn't found.
        If cache is provided and item is not in pool, attempt to get it from cache.
        """
        mp_key = self._lru.get(key)
        if mp_key is not None:
            return self._memory_pool.read(mp_key)

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
        if not self._memory_pool.can_commit(value):
            evicted_key, evicted_value = self._lru.evict(details=True)
            if evicted_key:
                self._memory_pool.release(evicted_value)
            else:
                return None  # Return None if no item could be evicted

        # Try to commit the value to the memory pool
        memory_pool_key = self._memory_pool.commit(value)
        if memory_pool_key is None:
            return None  # Return None if commit still fails after eviction

        # Update LRU cache with the new key and memory pool key if commit succeeds
        self._lru.set(key, memory_pool_key)

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
        self._lru.reset(reset_stats=reset_stats)


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
