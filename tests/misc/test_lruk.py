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
    assert cache.get("key1") == "value1", cache.get("key1")
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
    assert lru.get("c") is None, lru.get("c")
    assert lru.get("d") == 4
    assert lru.get("e") == 5
    assert lru.get("f") == 6


def test_get_non_existing_key_after_eviction():
    lru = LRU2(size=3)

    # Add 3 items to the cache
    lru.set("a", 1)
    lru.set("b", 2)
    lru.set("c", 3)

    # Add a fourth item to the cache, which should trigger an eviction of the least recently used item
    lru.set("d", 4)

    # Try to get a non-existing key
    assert lru.get("e") is None


def test_overwrite_existing_key():
    lru = LRU2(size=3)

    # Add 3 items to the cache
    lru.set("a", 1)
    lru.set("b", 2)
    lru.set("c", 3)

    # Overwrite key "b"
    lru.set("b", 20)

    # Check the new value of "b"
    assert lru.get("b") == 20


def test_handle_non_string_keys():
    lru = LRU2(size=3)

    # Add 3 items to the cache with integer keys
    lru.set(1, "a")
    lru.set(2, "b")
    lru.set(3, "c")

    # Check the values
    assert lru.get(1) == "a"
    assert lru.get(2) == "b"
    assert lru.get(3) == "c"


def test_evict_as_last_resort():
    """
    we only evict items with less than K accesses if we need to evict someting
    """
    lru = LRU2(size=3)

    for item in ("a", "b", "c", "d", "e", "f"):
        lru.set(item, item)

    # Check the values
    assert lru.get("a") is None
    assert lru.get("b") is None
    assert lru.get("c") is None
    assert lru.get("d") == "d"
    assert lru.get("e") == "e"
    assert lru.get("f") == "f"


def test_lru2_eviction_based_on_penultimate_access():
    lru = LRU2(k=2, size=3)

    # Add 3 items to the cache
    lru.set("a", 1)
    lru.set("b", 2)
    lru.set("c", 3)

    # Access "a" and "b" twice to update their access history
    lru.get("a")  # First access for "a"
    lru.get("b")  # First access for "b"
    lru.get("a")  # Second access for "a"
    lru.get("b")  # Second access for "b"

    # Add another item; "a" should be evicted since its penultimate access is the oldest
    # Note it's not "c" that is evicted
    evicted = lru.set("d", 4)

    # Verify that "c" was evicted and "a", "b", and "d" are in the cache
    assert evicted == "a", evicted
    assert lru.get("a") == None
    assert lru.get("b") == 2
    assert lru.get("c") is 3
    assert lru.get("d") == 4


def test_lru4_eviction_based_on_fourth_last_access():
    lru = LRU2(k=4, size=5)

    # Add 5 items to the cache
    lru.set("a", 1)
    lru.set("b", 2)
    lru.set("c", 3)
    lru.set("d", 4)
    lru.set("e", 5)

    # Access "a", "b", "c", and "d" multiple times to update their access history
    for _ in range(3):
        lru.get("a")
        lru.get("b")
        lru.get("c")
        lru.get("d")

    # Fourth access for "a" and "b", third for "c" and "d", and "e" is still at its first access
    lru.get("a")
    lru.get("b")

    # Add another item; "c" should be evicted as its fourth-most recent access is the oldest
    # This is NOT evicted "e", it only has one access at this point
    evicted = lru.set("f", 6)

    # Verify that "c" was evicted and the others are in the cache
    assert evicted == "c"
    assert lru.get("a") == 1
    assert lru.get("b") == 2
    assert lru.get("c") is None
    assert lru.get("d") == 4
    assert lru.get("e") == 5
    assert lru.get("f") == 6


def test_lru4_synthetic_access_on_eviction():
    lru = LRU2(k=4, size=5)

    # Add 5 items to the cache
    lru.set("a", "a")
    lru.set("b", "b")
    lru.set("c", "c")
    lru.set("d", "d")
    lru.set("e", "e")

    # Access "a", "b", "c", and "d" three times each
    for _ in range(3):
        lru.get("a")
        lru.get("b")
        lru.get("c")
        lru.get("d")

    # "e" has only one access at this point, "a" has the oldest fourth access
    # Add another item, triggering eviction check, evicting "a"
    # "e" isn't evicted yet as it's only have one access, but we add a "synthetic" access
    # so now it looks like it's had two accesses
    evicted = lru.set("f", "f")
    assert evicted == "a"
    assert lru.get("e") == "e"
    # Verify that no other item was evicted
    assert all(lru.get(key) is not None for key in ["b", "c", "d", "e"])

    # Add items and check items, triggering eviction checks and access updates
    # we're accessing "b" often, and never accessing "e"
    for item in ("b", "c", "b", "d", "b", "f", "b", "g", "b", "h", "b", "i", "b", "j"):
        lru.set(item, item)
        lru.get(item)

    # Now "e" should be evicted as it would have reached 4 accesses including synthetic ones
    assert lru.get("e") is None
    # "b" shouldn't be evicted because it's accessed often and never gets to the bottom
    # of the list to evict
    assert lru.get("b") == "b"


def test_fifo():
    """
    we only evict items with less than K accesses if we need to evict someting
    """
    lru = LRU2(size=2)

    for item in ("a", "b", "c"):
        lru.set(item, item)
    lru.get("b")
    lru.get("b")
    for item in ("d", "e", "f"):
        lru.set(item, item)

    # Check the values
    assert lru.get("a") is None
    assert lru.get("b") is None
    assert lru.get("c") is None
    assert lru.get("d") is None
    assert lru.get("e") == "e"
    assert lru.get("f") == "f"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
