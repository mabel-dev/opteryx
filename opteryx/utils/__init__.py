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

from cityhash import CityHash64


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


def peak(generator):  # type:ignore
    """
    peak an item off a generator, this may have undesirable consequences so
    only use if you also wrote the generator
    """
    try:
        item = next(generator)
    except StopIteration:  # pragma: no cover
        return None, []
    return item, itertools.chain([item], generator)


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
    """
    This is roughly twice as fast as the previous implementation which was roughly
    twice as fast as it's previous implementation.

    This has more room for improvement, particularly in the Base64 encoding part,
    but this currently isn't identified as a performance bottleneck, the last
    rewrite was incidental when writing tests for a hasher.
    """
    import random
    import struct
    from base64 import b64encode

    words: int = int(-(width * 0.75) // -8) + 1

    bytestring = struct.pack("=" + "Q" * words, *[random.getrandbits(64)] * words)
    return b64encode(bytestring).decode()[:width]


def unique_id():
    """create a short, random hexadecimal string, uniqueness not guaranteed"""
    return f"{hex(random.getrandbits(40))[2:]:0>10}"


def is_arm():
    """am I running on an ARM CPU?"""
    import platform

    return platform.machine() in ("armv7l", "aarch64")
