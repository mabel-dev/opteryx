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

import numpy
from orso.cityhash import CityHash64


def hasher(vals):
    """
    Quickly hash a list of string or numerics (i.e. intended for join hash table)

    This is roughly 2x faster than the previous implementation for lists of strings.

    Do note though, if you're micro-optimizing, this is faster to create but is
    slower for some Python functions to handle the result of, like 'sorted'.
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


def suggest_alternative(name, candidates):
    """
    Find closest match using a variation of Levenshtein Distance

    This implementation is limited to searching for distance less than three, is case
    insenstive and removes any non-alpha numeric characters.

    This is tuned for this use case of quickly identifying likely matches when a user
    is entering field or function names and may have minor typos, casing or punctuation
    mismatches with the source value.
    """
    from opteryx.third_party.mbleven import compare

    best_match_column = None
    best_match_score = 100

    name = "".join(char for char in name if char.isalnum())
    for raw, candidate in (
        (
            ca,
            "".join(ch for ch in ca if ch.isalnum()),
        )
        for ca in candidates
    ):
        my_dist = compare(candidate, name)
        if my_dist == 0:  # if we find an exact match, return that
            return raw
        if 0 <= my_dist < best_match_score:
            best_match_score = my_dist
            best_match_column = raw

    return best_match_column


def is_arm():
    """am I running on an ARM CPU?"""
    import platform

    return platform.machine() in ("armv7l", "aarch64")
