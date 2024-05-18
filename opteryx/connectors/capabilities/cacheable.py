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


import asyncio
from functools import wraps

from orso.cityhash import CityHash64

from opteryx.config import MAX_CACHE_EVICTIONS_PER_QUERY
from opteryx.config import MAX_CACHEABLE_ITEM_SIZE

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
    from opteryx.managers.cache import NullCache
    from opteryx.shared import BufferPool

    cache_manager = get_cache_manager()
    max_evictions = MAX_CACHE_EVICTIONS_PER_QUERY
    remote_cache = cache_manager.cache_backend
    if not remote_cache:
        # rather than make decisions - just use a dummy
        remote_cache = NullCache()

    buffer_pool = BufferPool()

    my_keys = set()

    @wraps(func)
    def wrapper(blob_name, statistics, **kwargs):
        nonlocal max_evictions

        key = hex(CityHash64(blob_name)).encode()
        my_keys.add(key)

        # try the buffer pool first
        result = buffer_pool.get(key)
        if result is not None:
            statistics.bufferpool_hits += 1
            return result

        # try the remote cache next
        result = remote_cache.get(key)
        if result is not None:
            statistics.remote_cache_hits += 1
            return result

        # Key is not in cache, execute the function and store the result in cache
        result = func(blob_name=blob_name, **kwargs)

        # Write the result to caches
        if max_evictions:

            if len(result) < buffer_pool.size // 10:
                evicted = buffer_pool.set(key, result)
                if evicted:
                    # if we're evicting items we just put in the cache, stop
                    if evicted in my_keys:
                        max_evictions = 0
                    else:
                        max_evictions -= 1
                    statistics.cache_evictions += 1

            if len(result) < MAX_CACHEABLE_ITEM_SIZE:
                remote_cache.set(key, result)
            else:
                statistics.cache_oversize += 1

        statistics.cache_misses += 1

        return result

    return wrapper


def async_read_thru_cache(func):
    """
    This is added to the reader by the binder.

    We check the faster buffer pool (local memory), then the cache (usually
    remote cache like memcached) before fetching from source.

    The async readers don't return the bytes, instead they return the
    read buffer reference for the bytes, which means we need to populate
    the read buffer and return the ref for these items.
    """
    # Capture the max_evictions value at decoration time
    from opteryx import get_cache_manager
    from opteryx.managers.cache import NullCache
    from opteryx.shared import BufferPool
    from opteryx.shared import MemoryPool

    cache_manager = get_cache_manager()
    max_evictions = MAX_CACHE_EVICTIONS_PER_QUERY
    remote_cache = cache_manager.cache_backend
    if not remote_cache:
        # rather than make decisions - just use a dummy
        remote_cache = NullCache()

    buffer_pool = BufferPool()

    my_keys = set()

    @wraps(func)
    async def wrapper(blob_name: str, statistics, pool: MemoryPool, **kwargs):
        nonlocal max_evictions

        key = hex(CityHash64(blob_name)).encode()
        my_keys.add(key)

        # try the buffer pool first
        result = buffer_pool.get(key, zero_copy=False)
        if result is not None:
            statistics.bufferpool_hits += 1
            ref = await pool.commit(result)  # type: ignore
            while ref is None:
                await asyncio.sleep(0.1)
                statistics.stalls_writing_to_read_buffer += 1
                ref = await pool.commit(result)  # type: ignore
                statistics.bytes_read += len(result)
            return ref

        # try the remote cache next
        result = remote_cache.get(key)
        if result is not None:
            statistics.remote_cache_hits += 1
            ref = await pool.commit(result)  # type: ignore
            while ref is None:
                await asyncio.sleep(0.1)
                statistics.stalls_writing_to_read_buffer += 1
                ref = await pool.commit(result)  # type: ignore
                statistics.bytes_read += len(result)
            return ref

        try:
            result = await func(blob_name=blob_name, statistics=statistics, pool=pool, **kwargs)
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            raise  # Optionally re-raise the error after logging it

        # Write the result to caches
        if max_evictions:
            # we set a per-query eviction limit
            buffer = await pool.read(result)  # type: ignore

            if len(buffer) < buffer_pool.size // 10:
                evicted = buffer_pool.set(key, buffer)
                if evicted:
                    # if we're evicting items we just put in the cache, stop
                    if evicted in my_keys:
                        max_evictions = 0
                    else:
                        max_evictions -= 1
                    statistics.cache_evictions += 1

            if len(buffer) < MAX_CACHEABLE_ITEM_SIZE:
                remote_cache.set(key, buffer)
            else:
                statistics.cache_oversize += 1

        statistics.cache_misses += 1

        return result

    return wrapper
