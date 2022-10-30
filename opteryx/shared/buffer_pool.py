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

    slots = "_lru"

    def __init__(self, size):
        self._lru = LRU2(size=size)

    def get(self, key, cache):
        value = self._lru.get(key)
        if value is not None:
            return io.BytesIO(value)
        if cache is not None:
            value = cache.get(key)
        return value

    def set(self, key, value, cache):
        value.seek(0, 0)
        key = self._lru.set(key, value.read())
        value.seek(0, 0)
        if cache is not None:
            cache.set(key, value)
        return key

    @property
    def stats(self):
        return self._lru.stats

    def reset(self, reset_stats: bool = False):
        self._lru.reset(reset_stats=reset_stats)


class BufferPool(_BufferPool):

    _kv = None

    def __new__(cls):
        if cls._kv is None:
            from opteryx import config

            LOCAL_BUFFER_POOL_SIZE = config.LOCAL_BUFFER_POOL_SIZE
            cls._kv = _BufferPool(size=LOCAL_BUFFER_POOL_SIZE)
        return cls._kv
