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

from opteryx.utils.lru_2 import LRU2


class _BufferPool:
    """
    This is the class that there is a single instance of.

    This is the Buffer Pool, currently implemented as a dictionary of buffers.
    """

    slots = "_lru"

    def __init__(self, size):
        self._lru = LRU2(size=size)

    def get(self, key, cache):
        """Retrieve an item from the pool, return None if the item isn't found"""
        value = self._lru.get(key)
        if value is not None:
            return io.BytesIO(value)
        if cache is not None:
            value = cache.get(key)
        return value

    def set(self, key, value, cache):
        """Put an item into the pool, evict an item if the pool is full"""
        value.seek(0, 0)
        evicted = self._lru.set(key, value.read())
        value.seek(0, 0)
        if cache is not None:
            cache.set(key, value)
        return evicted

    @property
    def stats(self):  # pragma: no cover
        """hit, miss and eviction statistics"""
        return self._lru.stats

    def reset(self, reset_stats: bool = False):
        """reset the statistics"""
        self._lru.reset(reset_stats=reset_stats)


class BufferPool(_BufferPool):

    _kv = None

    def __new__(cls):
        if cls._kv is None:
            # import here to avoid cicular imports
            from opteryx import config

            local_buffer_pool_size = config.LOCAL_BUFFER_POOL_SIZE
            cls._kv = _BufferPool(size=local_buffer_pool_size)
        return cls._kv
