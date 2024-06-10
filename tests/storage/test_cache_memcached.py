"""
Test the memcached cache by executing the same query repwtedly. The first time we ensure
the files are in the cache (they may or may not be) for the second time to definitely
'hit' the cache.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.tools import is_arm, is_mac, is_windows, skip_if


@skip_if(is_arm() or is_windows() or is_mac())
def test_memcached_cache():
    os.environ["OPTERYX_DEBUG"] = "1"
    os.environ["MAX_LOCAL_BUFFER_CAPACITY"] = "100"
    os.environ["MAX_CACHE_EVICTIONS_PER_QUERY"] = "4"

    import opteryx
    from opteryx import CacheManager
    from opteryx.managers.cache import MemcachedCache

    cache = MemcachedCache()
    cache._server.flush_all()
    opteryx.set_cache_manager(CacheManager(cache_backend=cache))

    conn = opteryx.Connection()

    # read the data ten times, this should populate the cache if it hasn't already
    for i in range(10):
        cur = conn.cursor()
        cur.execute("SELECT * FROM testdata.flat.ten_files;")

    # read the data again time, this should hit the cache
    cur = conn.cursor()
    cur.execute("SELECT * FROM testdata.flat.ten_files;")
    stats = cur.stats

    assert (
        cache.hits >= 11
    ), f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}, sets: {cache.sets}"
    assert (
        cache.skips == 0
    ), f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}, sets: {cache.sets}"
    assert (
        cache.errors == 0
    ), f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}, sets: {cache.sets}"

    assert stats["remote_cache_hits"] >= stats["blobs_read"], stats
    assert stats.get("cache_misses", 0) == 0, stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
