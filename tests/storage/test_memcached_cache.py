"""
Test the memcached cache by executing the same query twice. The first time we ensure
the files are in the cache (they may or may not be) for the second time to definitely 
'hit' the cache.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_memcached_cache():

    import opteryx
    from opteryx.storage.cache.memcached_cache import MemcachedCache

    cache = MemcachedCache(server="localhost:11211")

    # read the data once, this should populate the cache if it hasn't already
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM tests.data.tweets WITH(NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    # this test is not idempotent, it will be in different states depending on if its
    # already been run
    assert stats["cache_hits"] in (0, 2)
    assert stats["cache_misses"] in (0, 2)
    conn.close()

    # read the data a second time, this should hit the cache
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM tests.data.tweets WITH(NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 2, stats
    assert stats["cache_misses"] == 0, stats
    conn.close()


if __name__ == "__main__":  # pragma: no cover

    test_memcached_cache()
    print("✅ okay")
