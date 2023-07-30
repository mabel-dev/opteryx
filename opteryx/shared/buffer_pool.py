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
import io

from opteryx import config
from opteryx.utils.lru_2 import LRU2


class _BufferPool:
    """
    Buffer Pool is a class implementing a Least Recently Used (LRU) cache of buffers.
    """

    slots = "_lru"

    def __init__(self, size):
        self._lru = LRU2(size=size)

    def get(self, key, cache=None):
        """
        Retrieve an item from the pool, return None if the item isn't found.
        If cache is provided and item is not in pool, attempt to get it from cache.
        """
        value = self._lru.get(key)
        if value is not None:
            return io.BytesIO(value)
        elif cache is not None:
            return cache.get(key)
        else:
            return None

    def set(self, key, value, cache=None):
        """
        Put an item into the pool, evict an item if the pool is full.
        If a cache is provided, also set the value in the cache.
        """
        value.seek(0)
        evicted = self._lru.set(key, value.read())
        value.seek(0)
        if cache is not None:
            cache.set(key, value)
        return evicted

    @property
    def stats(self):
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
            local_buffer_pool_size = config.LOCAL_BUFFER_POOL_SIZE
            cls._instance = _BufferPool(size=local_buffer_pool_size)
        return cls._instance
