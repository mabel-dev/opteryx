"""
Test the memcached cache by executing the same query twice. The first time we ensure
the files are in the cache (they may or may not be) for the second time to definitely 
'hit' the cache.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, skip_if


@skip_if(is_arm() or is_windows() or is_mac())
def test_redis_cache():
    import opteryx
    from opteryx import CacheManager
    from opteryx.managers.cache import RedisCache

    cache = RedisCache()
    opteryx.set_cache_manager(
        CacheManager(cache_backend=cache, max_local_buffer_capacity=1, max_evictions_per_query=4)
    )

    # read the data once, this should populate the cache if it hasn't already
    cur = opteryx.query("SELECT * FROM testdata.flat.ten_files;")
    stats = cur.stats

    # read the data a second time, this should hit the cache
    cur = opteryx.query("SELECT * FROM testdata.flat.ten_files;")

    assert cache.hits > 11
    assert cache.misses < 12
    assert cache.skips == 0
    assert cache.errors == 0

    stats = cur.stats
    assert stats["cache_hits"] >= stats["blobs_read"]
    assert stats.get("cache_misses", 0) == 0, stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
