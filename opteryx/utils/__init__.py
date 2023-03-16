# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import itertools
import random

import numpy
from orso.cityhash import CityHash64


def hasher(vals):
    """
    Quickly hash a list of string or numerics (i.e. intended for join hash table)

    This is roughly 2x faster than the previous implementation for lists of strings.

    Do note though, if you're micro-optimizing, this is faster to create but is
    slower for some Python functions to handle, like 'sorted'.
    """
    if numpy.issubdtype(vals.dtype, numpy.character):
        return numpy.array([CityHash64(s.encode()) for s in vals], numpy.uint64)
    return vals


def peek(iterable):  # type:ignore
    """
    peek an item off a generator
    """
    iter1, iter2 = itertools.tee(iterable)
    try:
        first = next(iter1)
    except StopIteration:
        return None, iter([])
    else:
        return first, iter2


def fuzzy_search(name, candidates):
    """
    Find closest match using a Levenshtein Distance variation
    """
    from opteryx.third_party.mbleven import compare

    best_match_column = None
    best_match_score = 100

    for candidate in candidates:
        my_dist = compare(candidate, name)
        if 0 <= my_dist < best_match_score:
            best_match_score = my_dist
            best_match_column = candidate

    return best_match_column


def random_int() -> int:
    """
    Select a random integer (32bit)
    """
    return random.getrandbits(32)


def random_string(width: int = 16):
    num_chars = ((width + 1) >> 1) << 3  # Convert length to number of bits
    rand_bytes = random.getrandbits(num_chars)  # Generate random bytes
    rand_hex = hex(rand_bytes)[
        2 : width + 2
    ]  # Convert to hex string and truncate to desired length
    return rand_hex


def is_arm():
    """am I running on an ARM CPU?"""
    import platform

    return platform.machine() in ("armv7l", "aarch64")
