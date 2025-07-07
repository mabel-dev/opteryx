# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import asyncio
from functools import wraps

from opteryx.config import MAX_CACHE_EVICTIONS_PER_QUERY
from opteryx.config import MAX_CACHEABLE_ITEM_SIZE
from opteryx.third_party.cyan4973.xxhash import hash_bytes

__all__ = ("Cacheable", "async_read_thru_cache")

SOURCE_NOT_FOUND = 0
SOURCE_BUFFER_POOL = 1
SOURCE_REMOTE_CACHE = 2
SOURCE_ORIGIN = 3


class Cacheable:
    """
    This class is just a marker - it is empty.

    Caching is added in the binding phase.
    """

    def __init__(self, *args, **kwargs):
        pass

    def read_blob(self, *, blob_name, **kwargs):
        pass


def async_read_thru_cache(func):
    """
    This is added to the reader by the binder.

    We check the faster buffer pool (local memory), then the cache (usually
    remote cache like memcached) before fetching from source.

    The async readers don't return the bytes, instead they return the
    read buffer reference for the bytes, which means we need to populate
    the read buffer and return the ref for these items.
    """
    # Capture the evictions_remaining value at decoration time
    from opteryx import get_cache_manager
    from opteryx import system_statistics
    from opteryx.managers.cache import NullCache
    from opteryx.shared import BufferPool
    from opteryx.shared import MemoryPool

    cache_manager = get_cache_manager()
    evictions_remaining = MAX_CACHE_EVICTIONS_PER_QUERY
    remote_cache = cache_manager.cache_backend
    if not remote_cache:
        # rather than make decisions - just use a dummy
        remote_cache = NullCache()

    buffer_pool = BufferPool()

    my_keys = set()

    @wraps(func)
    async def wrapper(blob_name: str, statistics, pool: MemoryPool, **kwargs):
        try:
            nonlocal evictions_remaining

            source = SOURCE_NOT_FOUND
            key = hex(hash_bytes(blob_name.encode())).encode()
            read_buffer_ref = None
            payload = None
            my_keys.add(key)

            # try the buffer pool first
            payload = buffer_pool.get(key, zero_copy=True, latch=True)
            if payload is not None:
                source = SOURCE_BUFFER_POOL
                remote_cache.touch(key)  # help the remote cache track LRU
                statistics.bufferpool_hits += 1
                read_buffer_ref = await pool.commit(payload)  # type: ignore
                while read_buffer_ref == -1:  # pragma: no cover
                    await asyncio.sleep(0.1)
                    statistics.stalls_writing_to_read_buffer += 1
                    read_buffer_ref = await pool.commit(payload)  # type: ignore
                    statistics.bytes_read += len(payload)
                    system_statistics.cpu_wait_seconds += 0.1
                buffer_pool.unlatch(key)
                return read_buffer_ref

            # try the remote cache next
            payload = remote_cache.get(key)
            if payload is not None:
                source = SOURCE_REMOTE_CACHE
                statistics.remote_cache_hits += 1
                system_statistics.remote_cache_reads += 1
                read_buffer_ref = await pool.commit(payload)  # type: ignore
                while read_buffer_ref == -1:  # pragma: no cover
                    await asyncio.sleep(0.1)
                    statistics.stalls_writing_to_read_buffer += 1
                    read_buffer_ref = await pool.commit(payload)  # type: ignore
                    statistics.bytes_read += len(payload)
                    system_statistics.cpu_wait_seconds += 0.1
                return read_buffer_ref

            try:
                read_buffer_ref = await func(
                    blob_name=blob_name, statistics=statistics, pool=pool, **kwargs
                )
                source = SOURCE_ORIGIN
                statistics.cache_misses += 1
                system_statistics.origin_reads += 1
                return read_buffer_ref
            except Exception as e:  # pragma: no cover
                print(f"Error in {func.__name__}: {e}")
                raise  # Optionally re-raise the error after logging it

        finally:
            if payload is None and read_buffer_ref is not None:
                # we set a per-query eviction limit
                payload = await pool.read(read_buffer_ref)  # type: ignore

            # If we found the file, see if we need to write it to the caches
            if (
                not source in (SOURCE_NOT_FOUND, SOURCE_BUFFER_POOL)
                and evictions_remaining > 0
                and len(payload) < buffer_pool.size // 10
            ):
                # if we didn't get it from the buffer pool (origin or remote cache) we add it
                evicted = buffer_pool.set(key, payload)
                if evicted:  # pragma: no cover
                    # if we're evicting items we just put in the cache, stop
                    if evicted in my_keys:
                        evictions_remaining = -1
                    else:
                        evictions_remaining -= 1
                    statistics.cache_evictions += 1

            if payload is None:
                # If we didn't find the payload, we don't write it to the cache
                pass
            elif source == SOURCE_ORIGIN and len(payload) < MAX_CACHEABLE_ITEM_SIZE:
                # If we read from the source, it's not in the remote cache
                remote_cache.set(key, payload)
                system_statistics.remote_cache_commits += 1
            elif len(payload) >= MAX_CACHEABLE_ITEM_SIZE:
                statistics.cache_oversize += 1

    return wrapper
