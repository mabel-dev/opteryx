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

This is little more than a wrapper around the LRU-K(2) cache.
"""
from array import array
from typing import Any
from typing import Optional

from opteryx.utils.lru_2 import LRU2


class NullCacheBackEnd:
    """
    We can remove a check in each operation by just having a null service.
    """

    def get(self, key: str) -> None:
        return None

    def set(self, key: str, value: Any) -> None:
        return None


class _BufferPool:
    """
    Buffer Pool is a class implementing a Least Recently Used (LRU) cache of buffers.
    """

    slots = "_lru", "_cache_backend", "_max_cacheable_item_size"

    def __init__(self, cache_manager):
        self._cache_backend = cache_manager.cache_backend
        if not self._cache_backend:
            self._cache_backend = (
                NullCacheBackEnd()
            )  # rather than make decisions - just use a dummy
        self._max_cacheable_item_size = cache_manager.max_cacheable_item_size
        self._lru = LRU2(size=cache_manager.max_local_buffer_capacity)

    def get(self, key: str) -> Optional[array]:
        """
        Retrieve an item from the pool, return None if the item isn't found.
        If cache is provided and item is not in pool, attempt to get it from cache.
        """
        value = self._lru.get(key)
        if value is not None:
            return value
        return self._cache_backend.get(key)

    def set(self, key, value) -> Optional[str]:
        """
        Put an item into the pool, evict an item if the pool is full.
        If a cache is provided, also set the value in the cache.
        """
        if len(value) < self._max_cacheable_item_size:
            evicted = self._lru.set(key, value)
            self._cache_backend.set(key, value)
            return evicted

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
            # Import here to avoid circular imports
            from opteryx import cache_manager

            cls._instance = _BufferPool(cache_manager)
        return cls._instance
