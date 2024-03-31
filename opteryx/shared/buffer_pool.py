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
"""
from typing import Optional

from opteryx.managers.cache import NullCache
from opteryx.shared.memory_pool import MemoryPool
from opteryx.utils.lru_2 import LRU2


class _BufferPool:
    """
    Buffer Pool is a class implementing a Least Recently Used (LRU) policy.
    """

    slots = "_lru", "_cache_backend", "_max_cacheable_item_size", "_memory_pool"

    def __init__(self, cache_manager):
        self._cache_backend = cache_manager.cache_backend
        if not self._cache_backend:
            self._cache_backend = NullCache()  # rather than make decisions - just use a dummy
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
        return self._cache_backend.get(key)

    def set(self, key: bytes, value) -> Optional[str]:
        """
        Put an item into the pool, evict an item if the pool is full.
        If a cache is provided, also set the value in the cache.
        """
        # always try to save to the cache backend
        self._cache_backend.set(key, value)

        # try to save in the buffer pool, if we fail, release
        # an item from the pool (LRUK2) and try again
        evicted_key = None
        mp_key = self._memory_pool.commit(value)
        if mp_key is None:
            evicted_key, evicted_value = self._lru.evict(True)
            if evicted_key:
                self._memory_pool.release(evicted_value)
                mp_key = self._memory_pool.commit(value)
                if mp_key is None:
                    return None
        else:
            self._lru.set(key, mp_key)
        return evicted_key

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
        # Import here to avoid circular imports
        from opteryx import get_cache_manager

        cache_manager = get_cache_manager()

        return _BufferPool(cache_manager)

    @classmethod
    def reset(cls):
        """
        Reset the BufferPool singleton instance. This is useful when the configuration changes.
        """
        cls._instance = None
        cls._instance = cls._create_instance()
