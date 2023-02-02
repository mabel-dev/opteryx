import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_lruk():
    from opteryx.utils.lru_2 import LRU2

    # make it very small to test
    lru = LRU2(size=2)
    hits = 0
    misses = 0
    evictions = 0

    assert lru.stats == (hits, misses, evictions), lru.stats

    # if we get from an empty, we'll miss
    lru.get("two")
    misses += 1
    assert lru.stats == (hits, misses, evictions), lru.stats

    # if we put something in, we won't change the stats
    lru.set("one", io.BytesIO(b"1"))
    assert lru.stats == (hits, misses, evictions), lru.stats

    # if we get something we put in, we should get it back and inc the hit count
    assert lru.get("one").read() == b"1"
    hits += 1
    assert lru.stats == (hits, misses, evictions), lru.stats

    # if we put two new things in, we should evict 'one'
    lru.set("two", io.BytesIO(b"2"))
    lru.set("three", io.BytesIO(b"3"))
    evictions += 1
    assert lru.stats == (hits, misses, evictions), lru.stats

    # confirm 'one' was evicted
    lru.get("one")
    misses += 1
    assert lru.stats == (hits, misses, evictions), lru.stats

    # if we get 'two' it's the LRU, but not the LRU-K(2)
    # so if we add 'one', it should evict 'three'
    assert lru.get("two").read() == b"2"
    hits += 1
    lru.set("one", io.BytesIO(b"1"))
    evictions += 1
    assert lru.stats == (hits, misses, evictions), lru.stats
    lru.get("two")
    misses += 1
    assert lru.stats == (hits, misses, evictions), lru.stats


if __name__ == "__main__":  # pragma: no cover
    test_lruk()
    print("✅ okay")
