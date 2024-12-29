"""
Test the memcached cache by executing the same query repeatedly. The first time we ensure
the files are in the cache (they may or may not be) for the second time to definitely
'hit' the cache.
"""

import os
import sys

os.environ["OPTERYX_DEBUG"] = "1"

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, skip_if

import os
import threading
import time
from opteryx.managers.cache import MemcachedCache
from orso.tools import random_string

@skip_if(is_arm() or is_windows() or is_mac())
def test_memcached_cache():

    import opteryx
    from opteryx import CacheManager
    from opteryx.managers.cache import MemcachedCache
    from opteryx.shared import BufferPool

    cache = MemcachedCache()
    #cache._server.flush_all()
    opteryx.set_cache_manager(CacheManager(cache_backend=cache))

    conn = opteryx.Connection()

    # read the data ten times, this should populate the cache if it hasn't already
    for i in range(10):
        cur = conn.cursor()
        time.sleep(0.01)
        cur.execute("SELECT count(*) FROM testdata.flat.ten_files;")

    print(f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}, sets: {cache.sets}")

    for i in range(10):
        # read the data again time, this should hit the cache
        buffer = BufferPool()
        buffer.reset()

        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM testdata.flat.ten_files;")

    stats = cur.stats

    print(f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}, sets: {cache.sets}")
    print(stats)

    assert (
        cache.hits > 0
    ), f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}, sets: {cache.sets}"

    assert stats.get("remote_cache_hits", 0) >= stats["blobs_read"], str(stats)
    assert stats.get("cache_misses", 0) == 0, stats

@skip_if(is_arm() or is_windows() or is_mac())
def test_memcache_stand_alone():
    os.environ["OPTERYX_DEBUG"] = "1"
    from opteryx.managers.cache import MemcachedCache
    from orso.tools import random_string

    cache = MemcachedCache()

    payloads = [random_string().encode() for i in range(100)]

    for load in payloads:
        cache.set(load, load)

    for load in payloads:
        result = cache.get(load)
        if result:
            assert result == load, f"{result} != {load}"


def set_in_cache(cache: MemcachedCache, key: bytes, value: bytes):
    """Function to set a value in the cache."""
    cache.set(key, value)

def get_from_cache(cache: MemcachedCache, key: bytes):
    """Function to get a value from the cache."""
    time.sleep(0.01)
    result = cache.get(key)
    if result:
        assert result == key, f"{result} != {key}"

def threaded_cache_operations(cache: MemcachedCache, payloads: list):
    """Function to perform concurrent cache operations using threads."""
    threads = []

    # Create threads for setting and getting cache entries
    for load in payloads:
        t_set = threading.Thread(target=set_in_cache, args=(cache, load, load))
        t_get = threading.Thread(target=get_from_cache, args=(cache, load))
        threads.extend([t_set, t_get])

    # Start all threads
    for thread in threads:
        time.sleep(0.01)
        thread.start()

    # Join all threads
    for thread in threads:
        thread.join()

@skip_if(is_arm() or is_windows() or is_mac())
def test_memcache_threaded():
    os.environ["OPTERYX_DEBUG"] = "1"
    
    cache = MemcachedCache()
    payloads = [random_string().encode() for i in range(100)]

    # Perform threaded cache operations
    threaded_cache_operations(cache, payloads)

    # Sleep for a short while to allow any potential race conditions to manifest
    time.sleep(1)

    # Verify all entries
    for load in payloads:
        result = cache.get(load)
        if result:
            assert result == load, f"Post-thread check failed: {result} != {load}"

@skip_if(is_arm() or is_windows() or is_mac())
def test_skip_on_error():
    from opteryx.managers.cache import MemcachedCache
    cache = MemcachedCache()
    cache.set(b"key", b"value")
    assert cache.get(b"key") == b"value"
    cache._consecutive_failures = 10
    assert cache.get(b"key") is None


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
