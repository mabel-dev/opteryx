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

import heapq
import time
from collections import defaultdict


class LRU2:
    def __init__(self, k=2, size=50):
        self.k = k
        self.size = size
        self.cache = {}
        self.access_history = defaultdict(list)
        self.removed = set()
        self.heap = []
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key):
        # Check if the key is in the cache
        if key in self.cache:
            value = self.cache[key]
            self.hits += 1  # Increment the hit count
        else:
            value = None
            self.misses += 1  # Increment the miss count

        # Update the access history for the key
        if key in self.access_history:
            # Remove and append the access time to simulate recent access
            old_entry = self.access_history[key].pop(0)
            self.access_history[key].append((time.monotonic_ns(), key))
            # Remove the old access time from the set
            self.removed.add(old_entry)

        # return the value
        return value

    def set(self, key, value):
        # If the key is already in the cache, we need to first remove the old entry
        if key in self.cache:
            old_entry = self.access_history[key].pop(0)
            self.removed.add(old_entry)
        self.cache[key] = value
        access_time = time.monotonic_ns()
        self.access_history[key].append(access_time)
        heapq.heappush(self.heap, (access_time, key))
        while len(self.cache) > self.size:
            return self._evict()

    def _evict(self):
        while self.heap:
            oldest_access_time, oldest_key = heapq.heappop(self.heap)
            if (oldest_access_time, oldest_key) not in self.removed:
                self.cache.pop(oldest_key)
                self.access_history.pop(oldest_key)
                self.evictions += 1
                break
            self.removed.remove((oldest_access_time, oldest_key))
            return oldest_key

    @property
    def keys(self):
        return list(self.cache.keys())

    @property
    def stats(self):
        return self.hits, self.misses, self.evictions

    def reset(self, reset_stats=False):
        self.cache = {}
        self.access_history.clear()
        self.removed.clear()
        self.heap = []
        if reset_stats:
            self.hits = 0
            self.misses = 0
            self.evictions = 0
