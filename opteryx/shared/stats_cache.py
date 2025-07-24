# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Global Blob Statistics Cache.

This module implements a global statistics cache using an LRU-K2 policy to determine
which items to keep and which to evict. The cache is used to store and retrieve
table or query statistics efficiently, and is shared across all connections and cursors.

The cache is limited by the number of items (MAX_STATISTICS_CACHE_ITEMS), not by memory volume.
Eviction occurs when the item count exceeds the configured maximum.
"""

from typing import Optional

from opteryx.compiled.structures.lru_k import LRU_K
from opteryx.config import MAX_STATISTICS_CACHE_ITEMS
from opteryx.models import RelationStatistics


class _StatsCache:
    """
    Implements a statistics cache using an LRU-K2 eviction policy.
    Stores serialized statistics objects, keyed by bytes.
    """

    slots = "_lru"

    def __init__(self):
        self._lru = LRU_K(k=2)

    def get(self, key: bytes) -> Optional[RelationStatistics]:
        """
        Retrieve a statistics object from the cache by key.
        Returns the deserialized object if found, or None if not present.
        """
        cached_stats = self._lru.get(key)
        if cached_stats is not None:
            return RelationStatistics.from_bytes(cached_stats)
        return None

    def delete(self, key: bytes):
        """
        Remove a statistics object from the cache by key.
        """
        self._lru.delete(key)

    def set(self, key: bytes, value: RelationStatistics):
        """
        Store a statistics object in the cache, serializing it to bytes.
        If the cache exceeds the maximum allowed items, evict the least recently used item.

        Args:
            key: The key associated with the statistics object.
            value: The statistics object to store (will be serialized).
        """
        cached_stats = value.to_bytes()

        # Update LRU cache with the new key and memory pool key if commit succeeds
        self._lru.set(key, cached_stats)
        if self._lru.size > MAX_STATISTICS_CACHE_ITEMS:
            # If the cache size exceeds the limit, evict the least recently used item
            self._lru.evict()

    @property
    def stats(self) -> tuple:
        """
        Return the hit, miss, and eviction statistics for the cache.
        """
        return self._lru.stats

    def __del__(self):
        pass
        # DEBUG: print(f"Statistics Cache <hits={self.stats[0]}, misses={self.stats[1]}, evictions={self.stats[2]}, inserts={self.stats[3]}>")


class StatsCache(_StatsCache):
    """
    Singleton wrapper for the _StatsCache class.
    Ensures only one global statistics cache instance exists.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = cls._create_instance()
        return cls._instance

    @classmethod
    def _create_instance(cls):
        """
        Create a new instance of the underlying _StatsCache.
        """
        return _StatsCache()

    @classmethod
    def reset(cls):
        """
        Reset the StatsCache singleton instance. This is useful when the configuration changes.
        """
        cls._instance = None
        cls._instance = cls._create_instance()
