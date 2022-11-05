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

"""
LRU-K evicts the page whose K-th most recent access is furthest in the past.

This is a basic implementation of LRU-2, which evicts entries according to the time
of their penultimate access. The main benefit of this approach is to prevent
a problem when the items being checked exceeds the number of items in the cache. A
classic LRU will evict and repopulate the cache for every call. LRU-2 reduces the
likelihood of this, but not preferring the MRU item to be retained.

LRU-K should be used in conjunction with eviction limits per query - this appears to
broadly be the solution used by Postgres. This can be supported by the calling
function using the return from the .set call to determine if an item was evicted from
the cache.

This can also be used as the index for an external cache (for example in plasma), where
the set() returns the evicted item which the calling function can then evict from the
external cache.
"""

import time

import numpy


class LRU2:

    slots = ("_size", "_cache", "_hits", "_misses", "_evictions")

    def __init__(self, **kwargs):
        """
        Parameters:
            size: int (optional)
                The maximim number of items maintained in the cache, default is 50.
        """
        self._size = int(kwargs.get("size", 50))
        self._cache = {}
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get(self, key):
        # we're disposing of access_2 (the penultimate access), recording this access
        # as the latest and making the access_1 the new penultimate access
        (value, access_1, _) = self._cache.get(key, (None, None, None))
        if value is not None:
            self._cache[key] = (value, time.monotonic_ns(), access_1)
            self._hits += 1
            return value
        self._misses += 1
        return None

    def set(self, key, value):
        # if we're already in the cache - do nothing
        if key in self._cache:
            return None
        # create an initial entry for the new item
        clock = time.monotonic_ns()
        self._cache[key] = (value, clock, clock)

        # If we're  full, we want to remove an item from the cache.
        # We choose the item to remove based on the penultimate access for that item.
        if len(self._cache) > self._size:

            keys = tuple(self._cache.keys())
            accesses = (c[2] for c in self._cache.values())

            least_recently_used = numpy.argmin(accesses)
            evicted_key = keys[least_recently_used]

            self._cache.pop(evicted_key)
            self._evictions += 1
            return evicted_key
        return None

    @property
    def keys(self):
        return list(self._cache.keys())

    @property
    def stats(self):
        # return hits, misses, evictions
        return (self._hits, self._misses, self._evictions)

    def reset(self, reset_stats: bool = False):
        self._cache = {}
        if reset_stats:
            self._hits = 0
            self._misses = 0
            self._evictions = 0
