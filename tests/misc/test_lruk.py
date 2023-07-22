import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils.lru_2 import LRU2


def test_lruk():
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


def test_lru2():
    # initialize cache with size 2
    cache = LRU2(size=2)

    # add two items
    cache.set("key1", "value1")
    cache.set("key2", "value2")

    # make sure they are in cache
    assert cache.get("key1") == "value1"
    assert cache.get("key2") == "value2"

    # add another item - this should trigger an eviction of the least recently used item
    cache.set("key3", "value3")

    # make sure the least recently used item was evicted
    assert cache.get("key1") is None
    assert cache.get("key2") == "value2"
    assert cache.get("key3") == "value3"

    # add items until we exceed the cache size, this should trigger multiple evictions
    for i in range(4, 7):
        cache.set(f"key{i}", f"value{i}")

    # check that the least recently used items were evicted
    assert cache.get("key2") is None
    assert cache.get("key3") is None
    assert cache.get("key4") is None
    assert cache.get("key5") == "value5"
    assert cache.get("key6") == "value6"

    # check the cache stats
    assert cache.stats == (6, 4, 4), cache.stats

    # reset the cache and make sure it's empty
    cache.reset()
    assert cache.get("key3") is None
    assert len(cache.keys) == 0

    # test that reset_stats works
    cache.set("key1", "value1")
    cache.reset(reset_stats=True)
    assert cache.stats == (0, 0, 0)


def test_lru_cache_eviction():
    lru = LRU2(size=3)

    # Add 3 items to the cache
    lru.set("a", 1)
    lru.set("b", 2)
    lru.set("c", 3)

    # Check that all items are in the cache
    assert lru.get("a") == 1
    assert lru.get("b") == 2
    assert lru.get("c") == 3

    # Add a fourth item to the cache, which should trigger an eviction of the least recently used item
    lru.set("d", 4)

    # Check that the least recently used item 'a' has been evicted
    assert lru.get("a") is None
    assert lru.get("b") == 2
    assert lru.get("c") == 3
    assert lru.get("d") == 4

    # Add a fifth item to the cache, which should trigger another eviction
    lru.set("e", 5)

    # Check that the least recently used item 'b' has been evicted
    assert lru.get("a") is None
    assert lru.get("b") is None
    assert lru.get("c") == 3
    assert lru.get("d") == 4
    assert lru.get("e") == 5

    # Add a sixth item to the cache, which should trigger another eviction
    lru.set("f", 6)

    # Check that the least recently used item 'c' has been evicted
    assert lru.get("a") is None
    assert lru.get("b") is None
    assert lru.get("c") is None
    assert lru.get("d") == 4
    assert lru.get("e") == 5
    assert lru.get("f") == 6


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
