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
    import opteryx
    from opteryx import CacheManager
    from opteryx.managers.cache import MemcachedCache

    cache = MemcachedCache()
    opteryx.set_cache_manager(
        CacheManager(cache_backend=cache, max_local_buffer_capacity=1, max_evictions_per_query=4)
    )

    # read the data five times, this should populate the cache if it hasn't already
    for i in range(10):
        cur = opteryx.query("SELECT * FROM testdata.flat.ten_files;")

    # read the data again time, this should hit the cache
    cur = opteryx.query("SELECT * FROM testdata.flat.ten_files;")
    stats = cur.stats

    assert (
        cache.hits >= 11
    ), f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}"
    assert (
        cache.skips == 0
    ), f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}"
    assert (
        cache.errors == 0
    ), f"hits: {cache.hits}, misses: {cache.misses}, skips: {cache.skips}, errors: {cache.errors}"

    assert stats["remote_cache_hits"] >= stats["blobs_read"], stats
    # assert stats.get("cache_misses", 0) == 0, stats


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    test_memcached_cache()

    run_tests()
