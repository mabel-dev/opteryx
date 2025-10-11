"""
Test the valkey cache by executing the same query twice. The first time we ensure
the files are in the cache (they may or may not be) for the second time to definitely
'hit' the cache.
"""

import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests import is_arm, is_mac, is_windows, skip_if


@skip_if(is_arm() or is_windows() or is_mac())
def test_valkey_cache():
    os.environ["OPTERYX_DEBUG"] = "1"
    os.environ["MAX_LOCAL_BUFFER_CAPACITY"] = "10"
    os.environ["MAX_CACHE_EVICTIONS_PER_QUERY"] = "4"

    import opteryx
    from opteryx import CacheManager
    from opteryx.managers.cache import ValkeyCache
    from opteryx.shared import BufferPool
    from opteryx import register_store
    from opteryx.connectors import GcpCloudStorageConnector

    register_store("opteryx", GcpCloudStorageConnector)

    cache = ValkeyCache()
    opteryx.set_cache_manager(CacheManager(cache_backend=cache))

    # read the data once, this should populate the cache if it hasn't already
    cur = opteryx.query("SELECT * FROM opteryx.ten_files;")
    cur.fetchall()

    buffer = BufferPool()
    buffer.reset()

    # read the data a second time, this should hit the cache
    cur = opteryx.query("SELECT * FROM opteryx.ten_files;")

    assert cache.hits > 0, cache.hits
    assert cache.misses < 12
    assert cache.skips == 0
    assert cache.errors == 0

    stats = cur.stats
    assert stats.get("remote_cache_hits", 0) >= stats["blobs_read"], stats
    assert stats.get("cache_misses", 0) == 0, stats

def test_invalid_config():
    from opteryx.managers.cache import ValkeyCache

    with pytest.raises(Exception):
        ValkeyCache(server="")

    v = ValkeyCache(server=None)
    assert v._consecutive_failures == 10

@skip_if(is_arm() or is_windows() or is_mac())
def test_skip_on_error():
    from opteryx.managers.cache import ValkeyCache
    cache = ValkeyCache()
    cache.set(b"key", b"value")
    read_back = cache.get(b"key")
    assert read_back == b"value", read_back
    assert cache.hits > 0
    cache._consecutive_failures = 10
    assert cache.get(b"key") is None


@skip_if(is_arm() or is_windows() or is_mac())
def test_valkey_delete():
    from opteryx.managers.cache import ValkeyCache

    cache = ValkeyCache()
    cache.delete(b"key")
    cache.set(b"key", b"value")
    assert cache.get(b"key") == b"value"
    cache.delete(b"key")
    assert cache.get(b"key") is None

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
