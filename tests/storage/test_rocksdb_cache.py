"""
Test the in memory cache by executing the same query twice. The first time we 'miss'
the cache and load the files into the cache for the second time to 'hit'.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import shutil

import opteryx
from opteryx.managers.kvstores import RocksDB_KVStore
from opteryx.shared import BufferPool

from tests.tools import skip_on_partials


@skip_on_partials
def test_in_memory_cache():
    buffer = BufferPool()
    buffer.reset(True)

    shutil.rmtree("test.rocksdb", ignore_errors=True)
    cache = RocksDB_KVStore(location="test.rocksdb")

    # read the data once, this should populate the cache
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM testdata.flat.tweets WITH(NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 0
    assert stats["cache_misses"] == 2
    conn.close()

    # read the data a second time, this should hit the cache
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM testdata.flat.tweets WITH(NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 2
    assert stats["cache_misses"] == 0
    conn.close()

    # read the data with the no cache directive
    conn = opteryx.connect(cache=cache)
    cur = conn.cursor()
    cur.execute("SELECT * FROM testdata.flat.tweets WITH (NO_CACHE, NO_PARTITION);")
    for record in cur.fetchall():
        # we just want to make sure we consume the data
        pass
    stats = cur.stats
    assert stats["cache_hits"] == 0
    assert stats["cache_misses"] == 0
    conn.close()


if __name__ == "__main__":  # pragma: no cover
    test_in_memory_cache()
    print("✅ okay")
