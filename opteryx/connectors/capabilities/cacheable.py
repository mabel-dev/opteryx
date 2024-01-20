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


from functools import wraps

from orso.cityhash import CityHash64

__all__ = ("Cacheable", "read_thru_cache")


class Cacheable:
    """
    This class is just a marker - it is empty.

    Caching is added in the binding phase.
    """

    def __init__(self, *args, **kwargs):
        pass

    def read_blob(self, *, blob_name, **kwargs):
        pass


def read_thru_cache(func):
    """
    Decorator to implement a read-thru cache.

    It intercepts requests to read blobs and first looks them up in the in-memory
    cache (BufferPool) and optionally in a secondary cache (like MemcacheD or Redis).
    """

    # Capture the max_evictions value at decoration time
    from opteryx import get_cache_manager
    from opteryx.shared import BufferPool

    cache_manager = get_cache_manager()
    max_evictions = cache_manager.max_evictions_per_query

    buffer_pool = BufferPool()

    @wraps(func)
    def wrapper(blob_name, statistics, **kwargs):
        nonlocal max_evictions

        key = hex(CityHash64(blob_name)).encode()

        # Try to get the result from cache
        result = buffer_pool.get(key)

        if result is not None:
            statistics.cache_hits += 1
            return result

        # Key is not in cache, execute the function and store the result in cache
        result = func(blob_name=blob_name, **kwargs)

        # Write the result to cache
        if max_evictions:
            if len(result) < buffer_pool.max_cacheable_item_size:
                evicted = buffer_pool.set(key, result)
                if evicted:
                    statistics.cache_evictions += 1
                    max_evictions -= 1
            else:
                statistics.cache_oversize += 1

        statistics.cache_misses += 1

        return result

    return wrapper
