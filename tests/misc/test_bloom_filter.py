import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils import random_string
from opteryx.utils.bloom_filter import BloomFilter, _get_hash_count, _get_size

ITERATIONS: int = 50000


def test_bloom_filter():
    # first we populate the BloomFilter
    tokens = (random_string(48) for i in range(ITERATIONS))
    bf = BloomFilter()
    for token in tokens:
        bf.add(token)

    # then we test. 100% shouldn't match (we use different string lengths)
    # but we're a probabilistic filter so expect some false positives
    # we're configured for a 1% false positive rate
    tokens = (random_string(32) for i in range(ITERATIONS))
    collisions = 0
    for token in tokens:
        if token in bf:
            collisions += 1

    # this is approximately 1% false positive rate, we're going to test between
    # 0.5 and 1.5 because this is probabilistic so are unlikely to actually get 1%
    assert (ITERATIONS * 0.005) < collisions < (ITERATIONS * 0.015), (
        collisions / ITERATIONS
    )


def test_contains():
    bf = BloomFilter()
    bf.add("test")
    assert "test" in bf
    assert "nonexistent" not in bf


def test_get_size():
    assert _get_size(50000, 0.01) == 479253, _get_size(50000, 0.01)
    assert _get_size(100000, 0.1) == 479253


def test_get_hash_count():
    assert _get_hash_count(479253, 50000) == 7, _get_hash_count(479253, 50000)
    assert _get_hash_count(479253, 100000) == 3


def test_init():
    bf = BloomFilter()
    assert bf.filter_size == 479253
    assert bf.hash_count == 7
    assert len(bf.hash_seeds) == 7
    assert bf.bits.size == 479253, bf.bits.size
    assert sum(bf.bits.array) == 0


if __name__ == "__main__":  # pragma: no cover
    test_bloom_filter()
    test_contains()
    test_get_size()
    test_get_hash_count()
    test_init()

    print("✅ okay")
