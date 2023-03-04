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
LRU-K evicts the morsel whose K-th most recent access is furthest in the past.

This is a basic implementation of LRU-2, which evicts entries according to the time of their
penultimate access. The main benefit of this approach is to prevent a problem when the items being
getted exceeds the number of items in the cache. A classic LRU will evict and repopulate the cache
for every call. LRU-2 reduces the likelihood of this, but not preferring the MRU item to be
retained.

LRU-K should be used in conjunction with eviction limits per query - this appears to broadly be
the solution used by Postgres. This can be supported by the calling function using the return from
the .set call to determine if an item was evicted from the cache.

This can also be used as the index for an external cache (for example in plasma), where the set()
returns the evicted item which the calling function can then evict from the external cache.
"""

import time
from collections import defaultdict


class LRU2:
    def __init__(self, k=2, size=50):
        self.k = k
        self.size = size
        self.cache = {}
        self.access_history = defaultdict(list)
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key):
        # Check if the key is in the cache
        if key in self.cache:
            # Update the access history for the key
            self.access_history[key].append(time.monotonic_ns())
            if len(self.access_history[key]) > self.k:
                self.access_history[key].pop(0)
            # Increment the hit count and return the value
            self.hits += 1
            return self.cache[key]
        else:
            # Increment the miss count and return None
            self.misses += 1
            return None

    def set(self, key, value):
        # Check if the key is already in the cache
        if key in self.cache:
            return None

        # Add the new key-value pair to the cache
        self.cache[key] = value

        # Update the access history for the key
        self.access_history[key].append(time.monotonic_ns())

        # If the cache is full, evict the least recently used item
        if len(self.cache) > self.size:
            # Find the key with the oldest access time and remove it from the cache
            oldest_key = None
            oldest_access_time = float("inf")
            for k in self.cache:
                if self.access_history[k][0] < oldest_access_time:
                    oldest_key = k
                    oldest_access_time = self.access_history[k][0]
            self.cache.pop(oldest_key)
            self.access_history.pop(oldest_key)
            self.evictions += 1
            return oldest_key

        return None

    @property
    def keys(self):  # pragma: no-cover
        return list(self.cache.keys())

    @property
    def stats(self):
        # return hits, misses, evictions
        return (self.hits, self.misses, self.evictions)

    def reset(self, reset_stats: bool = False):
        self.cache = {}
        if reset_stats:
            self.hits = 0
            self.misses = 0
            self.evictions = 0
