import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.utils import random_string
from opteryx.utils.bloom_filter import BloomFilter

ITERATIONS: int = 50000


def test_bloom_filter():

    # first we populate the BloomFilter
    tokens = (random_string(48) for i in range(ITERATIONS))
    bf = BloomFilter()
    for token in tokens:
        bf.add(token)

    # then we test. 100% shouldn't match (we use different string lengths)
    tokens = (random_string(32) for i in range(ITERATIONS))
    collisions = 0
    for token in tokens:
        if token in bf:
            collisions += 1

    # this is approximately 1% false positive rate, we're going to test between
    # 0.5 and 1.5 because this is probabilistic
    assert (ITERATIONS * 0.005) < collisions < (ITERATIONS * 0.015), (
        collisions / ITERATIONS
    )


if __name__ == "__main__":  # pragma: no cover

    test_bloom_filter()

    print("✅ okay")
