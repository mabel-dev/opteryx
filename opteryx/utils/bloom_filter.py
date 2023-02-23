"""
Bloom Filter

This is a variation of the Bloom Filter; this provides a fast and memory efficient way
to tell if an item is in a list.

This is a probabilistic algorithm, it saves memory and/or time to give you an
approximation of the correct answer. It doesn't claim to be 100% correct 100% of the
time.

https://en.wikipedia.org/wiki/Bloom_filter

(C) 2021-2023 Justin Joyce.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from cityhash import CityHash32
from opteryx.utils.bitarray.bitarray import bitarray

HASH_SEEDS = (
    "ANTHROPOMORPHISM",
    "BLOODYMINDEDNESS",
    "CHARACTERIZATION",
    "CONTEMPTUOUSNESS",
    "DISFRANCHISEMENT",
    "DISINGENUOUSNESS",
    "ELECTROTECHNICAL",
    "HYPERVENTILATION",
    "INCOMPREHENSIBLE",
    "NONRECIPROCATING",
    "ONEQUINTILLIONTH",
    "PRESUMPTUOUSNESS",
    "QUINTESSENTIALLY",
    "SENSATIONALISTIC",
    "THREEDIMENSIONAL",
    "UNCOMPREHENSIBLE",
    "UNDIPLOMATICALLY",
    "UNUNDERSTANDABLY",
)


def _log(x):
    return 99999999 * (x ** (1 / 99999999) - 1)


def _get_size(number_of_elements: int, fp_rate: float) -> int:
    """
    Calculate the size of the bitarray

    Parameters:
        number_of_elements: integer
            The number of items expected to be stored in filter
        fp_rate: float (optional)
            False Positive rate (0 to 1), default 0.05

    Returns:
        integer
    """
    size = -(number_of_elements * _log(fp_rate)) / (_log(2) ** 2) + 1
    return int(size)


def _get_hash_count(filter_size, number_of_elements):
    """
    Calculate the number of hashes to use to identify elements

    Parameters:
        filter_size: integer
            The size of the filter bit array
        number_of_elements: integer
            The number of items expected to be stored in filter

    Returns:
        integer
    """
    k = (filter_size / number_of_elements) * _log(2)
    return max(round(k), 2)


class BloomFilter:
    __slots__ = ("filter_size", "bits", "hash_seeds", "hash_count")

    def __init__(self, number_of_elements: int = 50000, fp_rate: float = 0.01):
        """
        Bloom Filters are a probabilistic approach to tracking items in a list.
        They use an array of booleans which are set according to hashes of the
        data items. Items are considered to be in the list if the booleans at
        the hashes are set, this results in a degree of false positives. This
        is factored into the calculation of the size of the boolean array and
        the number of hashes.

        This is used in the profiler to track unique string values without
        having to store the values or hashes of the values (minor errors
        with this count is not expected to be a problem)
        """
        self.filter_size: int = _get_size(number_of_elements, fp_rate)
        self.hash_count: int = _get_hash_count(self.filter_size, number_of_elements)
        self.hash_seeds: tuple = tuple(HASH_SEEDS[i] for i in range(self.hash_count))
        self.bits = bitarray(self.filter_size)

    def add(self, term):
        """
        Add a value to the index, returns true if the item is new, false if seen before
        """
        bits = self.bits

        for seed in self.hash_seeds:
            hash_ = CityHash32(f"{seed}{term}") % self.filter_size
            bits.set(hash_, 1)

    def __contains__(self, term):
        for seed in self.hash_seeds:
            hash_ = CityHash32(f"{seed}{term}") % self.filter_size
            if self.bits.get(hash_) == 0:
                return False
        return True

    def __repr__(self):  # pragma: no cover
        return f"BloomFilter <bits:{self.filter_size}, hashes:{self.hash_count}>"


if __name__ == "__main__":  # pragma: no cover
    b = BloomFilter()
    import random
    import time

    def unique_id():
        return f"{hex(random.getrandbits(40))}"

    n = time.monotonic_ns()
    for i in range(1000000):
        b.add(unique_id())
    print((time.monotonic_ns() - n) / 1e9)
