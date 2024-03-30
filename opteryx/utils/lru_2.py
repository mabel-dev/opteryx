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
LRU-K evicts the morsel whose K-th most recent access is furthest in the past. Note, the
LRU doesn't evict, it has no "size", the caller decides when the cache is full, this
could be slot/count based (100 items), or this could be volume-based (32Mb).

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

This is a variation of LRU-K, where an item has fewer than K accesses, it is not evicted unless
all items have fewer than K accesses. Not being evicted adds an access to age out single-hit items
from the cache. The resulting cache provides opportunity for novel items to prove their value before
being evicted.

If n+1 items are put into the cache in the same 'transaction', it acts like a FIFO - although
the BufferPool implements limit to only evict up to 32 items per 'transaction'
"""

import heapq
import time
from collections import defaultdict


class LRU2:
    def __init__(self, k=2):
        self.k = k
        self.slots = {}
        self.access_history = defaultdict(list)
        self.removed = set()
        self.heap = []

        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.inserts = 0

    def __len__(self):
        return len(self.slots)

    def get(self, key: bytes):
        value = self.slots.get(key)
        if value is not None:
            self.hits += 1
            self._update_access_history(key)
        else:
            self.misses += 1
        return value

    def set(self, key: bytes, value):
        self.inserts += 1
        self.slots[key] = value
        self._update_access_history(key)
        return None

    def _update_access_history(self, key: bytes):
        access_time = time.monotonic_ns()
        if len(self.access_history[key]) == self.k:
            old_entry = self.access_history[key].pop(0)
            self.removed.add(old_entry)
        self.access_history[key].append((access_time, key))
        heapq.heappush(self.heap, (access_time, key))

    def evict(self, details=False):
        while self.heap:
            oldest_access_time, oldest_key = heapq.heappop(self.heap)
            if (oldest_access_time, oldest_key) in self.removed:
                self.removed.remove((oldest_access_time, oldest_key))
                continue

            if len(self.access_history[oldest_key]) == 1:
                # Synthetic access to give a grace period
                new_access_time = time.monotonic_ns()
                self.access_history[oldest_key].append((new_access_time, oldest_key))
                heapq.heappush(self.heap, (new_access_time, oldest_key))
                continue

            # Evict the key with the oldest k-th access
            if oldest_key not in self.slots:
                continue
            value = self.slots.pop(oldest_key)
            self.access_history.pop(oldest_key)
            self.evictions += 1
            if details:
                return oldest_key, value
            return oldest_key

        if details:
            return None, None  # No item was evicted
        return None

    @property
    def keys(self):
        return list(self.slots.keys())

    @property
    def stats(self):
        return self.hits, self.misses, self.evictions, self.inserts

    def reset(self, reset_stats=False):
        self.slots = {}
        self.access_history.clear()
        self.removed.clear()
        self.heap = []
        if reset_stats:
            self.hits = 0
            self.misses = 0
            self.evictions = 0
            self.inserts = 0
