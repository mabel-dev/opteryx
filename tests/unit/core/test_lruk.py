import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.structures.lru_k import LRU_K
from tests import is_windows, skip_if


@skip_if(is_windows())
def test_lruk():
    # make it very small to test
    lru = LRU_K()
    hits = 0
    misses = 0
    evictions = 0
    inserts = 0

    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # if we get from an empty, we'll miss
    lru.get(b"two")
    misses += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # if we put something in, we won't change the stats
    lru.set(b"one", io.BytesIO(b"1").read())
    inserts += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # if we get something we put in, we should get it back and inc the hit count
    assert lru.get(b"one") == b"1"
    hits += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # if we put two new things in, we should evict 'one'
    lru.set(b"two", io.BytesIO(b"2").read())
    lru.set(b"three", io.BytesIO(b"3").read())
    inserts += 2
    evicted = lru.evict()
    evictions += 1
    assert evicted == b"one", evicted
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # confirm 'one' was evicted
    lru.get(b"one")
    misses += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats

    # if we get 'two' it's the LRU, but not the LRU-K(2)
    # so if we add 'one', it should evict 'three'
    assert lru.get(b"two") == b"2"
    hits += 1
    lru.set(b"one", io.BytesIO(b"1").read())
    inserts += 1
    evicted = lru.evict()
    evictions += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats
    assert evicted == b"two"
    lru.get(b"two")
    misses += 1
    assert lru.stats == (hits, misses, evictions, inserts), lru.stats


def test_lru2():
    cache = LRU_K(k=2)

    # add two items
    cache.set(b"key1", b"value1")
    cache.set(b"key2", b"value2")

    # make sure they are in cache
    assert cache.get(b"key1") == b"value1", cache.get(b"key1")
    assert cache.get(b"key2") == b"value2"

    # add another item - this should trigger an eviction of the least recently used item
    cache.set(b"key3", b"value3")
    evicted = cache.evict()

    # make sure the least recently used item was evicted
    assert evicted == b"key1"
    assert cache.get(b"key1") is None
    assert cache.get(b"key2") == b"value2"
    assert cache.get(b"key3") == b"value3"

    # add items until we exceed the cache size, this should trigger multiple evictions
    for i in range(4, 7):
        cache.set(f"key{i}".encode(), f"value{i}".encode())
        cache.evict()

    # check that the least recently used items were evicted
    assert cache.get(b"key2") is None
    assert cache.get(b"key3") is None
    assert cache.get(b"key4") is None
    assert cache.get(b"key5") == b"value5"
    assert cache.get(b"key6") == b"value6"

    # check the cache stats
    assert cache.stats == (6, 4, 4, 6), cache.stats

    # reset the cache and make sure it's empty
    cache.reset()
    assert cache.get(b"key3") is None
    assert len(cache.keys) == 0

    # test that reset_stats works
    cache.set(b"key1", b"value1")
    cache.reset(reset_stats=True)
    assert cache.stats == (0, 0, 0, 0)


def test_lru_cache_eviction():
    lru = LRU_K()

    # Add 3 items to the cache
    lru.set(b"a", b"1")
    lru.set(b"b", b"2")
    lru.set(b"c", b"3")

    # Check that all items are in the cache
    assert lru.get(b"a") == b"1"
    assert lru.get(b"b") == b"2"
    assert lru.get(b"c") == b"3"

    # Add a fourth item to the cache, which should trigger an eviction of the least recently used item
    lru.set(b"d", b"4")
    lru.evict()

    # Check that the least recently used item 'a' has been evicted
    assert lru.get(b"a") is None
    assert lru.get(b"b") == b"2"
    assert lru.get(b"c") == b"3"
    assert lru.get(b"d") == b"4"

    # Add a fifth item to the cache, which should trigger another eviction
    lru.set(b"e", b"5")
    lru.evict()

    # Check that the least recently used item 'b' has been evicted
    assert lru.get(b"a") is None
    assert lru.get(b"b") is None
    assert lru.get(b"c") == b"3"
    assert lru.get(b"d") == b"4"
    assert lru.get(b"e") == b"5"

    # Add a sixth item to the cache, which should trigger another eviction
    lru.set(b"f", b"6")
    lru.evict()

    # Check that the least recently used item 'c' has been evicted
    assert lru.get(b"a") is None
    assert lru.get(b"b") is None
    assert lru.get(b"c") is None, lru.get(b"c")
    assert lru.get(b"d") == b"4"
    assert lru.get(b"e") == b"5"
    assert lru.get(b"f") == b"6"


def test_get_non_existing_key_after_eviction():
    lru = LRU_K()

    # Add 3 items to the cache
    lru.set(b"a", b"1")
    lru.set(b"b", b"2")
    lru.set(b"c", b"3")

    # Add a fourth item to the cache, which should trigger an eviction of the least recently used item
    lru.set(b"d", b"4")
    lru.evict()

    # Try to get a non-existing key
    assert lru.get(b"e") is None


def test_overwrite_existing_key():
    lru = LRU_K()

    # Add 3 items to the cache
    lru.set(b"a", b"1")
    lru.set(b"b", b"2")
    lru.set(b"c", b"3")

    # Overwrite key "b"
    lru.set(b"b", b"20")

    # Check the new value of "b"
    assert lru.get(b"b") == b"20"


def test_evict_as_last_resort():
    """
    we only evict items with less than K accesses if we need to evict someting
    """
    lru = LRU_K()

    for item in (b"a", b"b", b"c", b"d", b"e", b"f"):
        lru.set(item, item)

    lru.evict()
    lru.evict()
    lru.evict()

    # Check the values
    assert lru.get(b"a") is None
    assert lru.get(b"b") is None
    assert lru.get(b"c") is None
    assert lru.get(b"d") == b"d"
    assert lru.get(b"e") == b"e"
    assert lru.get(b"f") == b"f"


def test_lru2_eviction_based_on_penultimate_access():
    lru = LRU_K(k=2)

    # Add 3 items to the cache
    lru.set(b"a", b"1")
    lru.set(b"b", b"2")
    lru.set(b"c", b"3")

    # Access "a" and "b" twice to update their access history
    lru.get(b"a")  # First access for "a"
    lru.get(b"b")  # First access for "b"
    lru.get(b"a")  # Second access for "a"
    lru.get(b"b")  # Second access for "b"

    # Add another item; "a" should be evicted since its penultimate access is the oldest
    # Note it's not "c" that is evicted
    lru.set(b"d", b"4")
    evicted = lru.evict()

    # Verify that "c" was evicted and "a", "b", and "d" are in the cache
    assert evicted == b"a", evicted
    assert lru.get(b"a") == None
    assert lru.get(b"b") == b"2"
    assert lru.get(b"c") == b"3"
    assert lru.get(b"d") == b"4"


@skip_if(is_windows())
def test_lru4_eviction_based_on_fourth_last_access():
    lru = LRU_K(k=4)

    # Add 5 items to the cache
    lru.set(b"a", b"1")
    lru.set(b"b", b"2")
    lru.set(b"c", b"3")
    lru.set(b"d", b"4")
    lru.set(b"e", b"5")

    # Access "a", "b", "c", and "d" multiple times to update their access history
    for _ in range(3):
        lru.get(b"a")
        lru.get(b"b")
        lru.get(b"c")
        lru.get(b"d")

    # Fourth access for "a" and "b", third for "c" and "d", and "e" is still at its first access
    lru.get(b"a")
    lru.get(b"b")

    # Add another item; "c" should be evicted as its fourth-most recent access is the oldest
    # This is NOT evicted "e", it only has one access at this point
    lru.set(b"f", b"6")
    evicted = lru.evict()

    # Verify that "c" was evicted and the others are in the cache
    assert evicted == b"c"
    assert lru.get(b"a") == b"1"
    assert lru.get(b"b") == b"2"
    assert lru.get(b"c") is None
    assert lru.get(b"d") == b"4"
    assert lru.get(b"e") == b"5"
    assert lru.get(b"f") == b"6"


@skip_if(is_windows())
def test_lru4_synthetic_access_on_eviction():
    lru = LRU_K(k=4)

    # Add 5 items to the cache
    lru.set(b"a", b"a")
    lru.set(b"b", b"b")
    lru.set(b"c", b"c")
    lru.set(b"d", b"d")
    lru.set(b"e", b"e")

    # Access "a", "b", "c", and "d" three times each
    for _ in range(3):
        lru.get(b"a")
        lru.get(b"b")
        lru.get(b"c")
        lru.get(b"d")

    # "e" has only one access at this point, "a" has the oldest fourth access
    # Add another item, triggering eviction check, evicting "a"
    # "e" isn't evicted yet as it's only have one access, but we add a "synthetic" access
    # so now it looks like it's had two accesses
    lru.set(b"f", b"f")
    evicted = lru.evict()
    assert evicted == b"a"
    assert lru.get(b"e") == b"e"
    # Verify that no other item was evicted
    assert all(lru.get(key) is not None for key in [b"b", b"c", b"d", b"e"])

    # Add items and check items, triggering eviction checks and access updates
    # we're accessing "b" often, and never accessing "e"
    for item in (b"b", b"c", b"b", b"d", b"b", b"f", b"b", b"g", b"b", b"h", b"b", b"i", b"b", b"j"):
        lru.set(item, item)
        lru.get(item)
        if len(lru) > 5:
            lru.evict()

    # Now "e" should be evicted as it would have reached 4 accesses including synthetic ones
    assert lru.get(b"e") is None
    # "b" shouldn't be evicted because it's accessed often and never gets to the bottom
    # of the list to evict
    assert lru.get(b"b") == b"b"


def test_fifo():
    """
    we only evict items with less than K accesses if we need to evict someting
    """
    lru = LRU_K()

    for item in (b"a", b"b", b"c"):
        lru.set(item, item)
        if len(lru) > 2:
            lru.evict()
    lru.get(b"b")
    lru.get(b"b")
    for item in (b"d", b"e", b"f"):
        lru.set(item, item)
        if len(lru) > 2:
            lru.evict()

    # Check the values
    assert lru.get(b"a") is None
    assert lru.get(b"b") is None
    assert lru.get(b"c") is None
    assert lru.get(b"d") is None
    assert lru.get(b"e") == b"e"
    assert lru.get(b"f") == b"f"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    import time
    start_time = time.perf_counter_ns()
    for i in range(1000):
        run_tests()
    end_time = time.perf_counter_ns()
    print(f"Tests completed in {(end_time - start_time) / 1e9}")
