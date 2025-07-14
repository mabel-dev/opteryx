"""
Test the redis cache by executing the same query twice. The first time we ensure
the files are in the cache (they may or may not be) for the second time to definitely
'hit' the cache.
"""

import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, skip_if


@skip_if(is_arm() or is_windows() or is_mac())
def test_redis_cache():
    os.environ["OPTERYX_DEBUG"] = "1"
    os.environ["MAX_LOCAL_BUFFER_CAPACITY"] = "10"
    os.environ["MAX_CACHE_EVICTIONS_PER_QUERY"] = "4"

    import opteryx
    from opteryx import CacheManager
    from opteryx.managers.cache import RedisCache
    from opteryx.shared import BufferPool
    from opteryx import register_store
    from opteryx.connectors import GcpCloudStorageConnector

    register_store("opteryx", GcpCloudStorageConnector)

    cache = RedisCache()
    opteryx.set_cache_manager(CacheManager(cache_backend=cache))

    # read the data once, this should populate the cache if it hasn't already
    cur = opteryx.query("SELECT * FROM opteryx.ten_files;")
    cur.fetchall()

    buffer = BufferPool()
    buffer.reset()

    # read the data a second time, this should hit the cache
    cur = opteryx.query("SELECT * FROM opteryx.ten_files;")

    assert cache.hits > 0
    assert cache.misses < 12
    assert cache.skips == 0
    assert cache.errors == 0

    stats = cur.stats
    assert stats.get("remote_cache_hits", 0) >= stats["blobs_read"], stats
    assert stats.get("cache_misses", 0) == 0, stats


def test_invalid_config():
    from opteryx.managers.cache import RedisCache

    with pytest.raises(Exception):
        RedisCache(server="")

    v = RedisCache(server=None)
    assert v._consecutive_failures == 10

@skip_if(is_arm() or is_windows() or is_mac())
def test_skip_on_error():
    from opteryx.managers.cache import RedisCache
    cache = RedisCache()
    cache.set(b"key", b"value")
    assert cache.get(b"key") == b"value"
    cache._consecutive_failures = 10
    assert cache.get(b"key") is None

@skip_if(is_arm() or is_windows() or is_mac())
def test_redis_delete():
    from opteryx.managers.cache import RedisCache

    cache = RedisCache()
    cache.delete(b"key")
    cache.set(b"key", b"value")
    assert cache.get(b"key") == b"value"
    cache.delete(b"key")
    assert cache.get(b"key") is None

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
