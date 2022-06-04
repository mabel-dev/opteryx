"""
Test the in memory cache by executing the same query twice. The first time we 'miss'
the cache and load the files into the cache for the second time to 'hit'.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_in_memory_cache():

    import opteryx
    from opteryx.storage.cache.memory_cache import InMemoryCache
    from opteryx.storage.adapters import DiskStorage

    cache = InMemoryCache(size=5)

    # read the data once, this should populate the cache
    conn = opteryx.connect(reader=DiskStorage(), cache=cache, partition_scheme=None)
    cur = conn.cursor()
    cur.execute("SELECT * FROM tests.data.tweets;")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 0
    assert stats["cache_misses"] == 2
    conn.close()

    # read the data a second time, this should hit the cache
    conn = opteryx.connect(reader=DiskStorage(), cache=cache, partition_scheme=None)
    cur = conn.cursor()
    cur.execute("SELECT * FROM tests.data.tweets;")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 2
    assert stats["cache_misses"] == 0
    conn.close()


if __name__ == "__main__":  # pragma: no cover

    test_in_memory_cache()
