"""
Test the in memory cache by executing the same query twice. The first time we 'miss'
the cache and load the files into the cache for the second time to 'hit'.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.managers.kvstores import LocalKVJson
from opteryx.shared import BufferPool


def test_json_cache():
    buffer = BufferPool()
    buffer.reset(True)

    if os.path.isfile("test.json"):  # pragma: no cover
        os.remove("test.json")
    cache = LocalKVJson(location="test.json")

    # read the data once, this should populate the cache
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM testdata.tweets WITH(NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data, doing it like this doesn't
        # waste memory as much
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 0, stats["cache_hits"]
    assert stats["cache_misses"] == 2
    conn.close()

    # read the data a second time, this should hit the cache
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM testdata.tweets WITH(NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 2, stats["cache_hits"]
    assert stats["cache_misses"] == 0
    conn.close()

    # read the data with the no cache directive
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM testdata.tweets WITH (NO_CACHE, NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 0
    assert stats["cache_misses"] == 0
    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_json_cache()
    print("✅ okay")
