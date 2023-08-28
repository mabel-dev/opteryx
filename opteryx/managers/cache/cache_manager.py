from typing import Union

from opteryx import config
from opteryx.exceptions import InvalidConfigurationError
from opteryx.managers.kvstores import BaseKeyValueStore

MAX_CACHEABLE_ITEM_SIZE = config.MAX_CACHEABLE_ITEM_SIZE
MAX_CACHE_EVICTIONS_PER_QUERY = config.MAX_CACHE_EVICTIONS_PER_QUERY
MAX_LOCAL_BUFFER_CAPACITY = config.MAX_LOCAL_BUFFER_CAPACITY


class CacheManager:
    """
    Manages cache behavior for Opteryx, including the cache store, item size, eviction policies, etc.

    Parameters:
        cache_backend: Union[BaseKeyValueStore, None]
            The cache storage to use.
        max_cacheable_item_size: int
            The maximum size a single item in the cache can occupy.
        max_evictions_per_query: int
            The number of items to evict from cache per query.
        max_local_buffer_capacity: int
            The maximum number of items to store in the BufferPool.
    """

    def __init__(
        self,
        cache_backend: Union[BaseKeyValueStore, None] = None,
        max_cacheable_item_size: Union[int, None] = MAX_CACHEABLE_ITEM_SIZE,
        max_evictions_per_query: Union[int, None] = MAX_CACHE_EVICTIONS_PER_QUERY,
        max_local_buffer_capacity: int = MAX_LOCAL_BUFFER_CAPACITY,
    ):
        if cache_backend is not None and not isinstance(cache_backend, BaseKeyValueStore):
            raise InvalidConfigurationError(
                config_item="cache_backend",
                provided_value=str(type(cache_backend)),
                valid_value_description="Instance of BaseKeyValueStore",
            )

        if max_cacheable_item_size is not None and (
            not isinstance(max_cacheable_item_size, int) or max_cacheable_item_size <= 0
        ):
            raise InvalidConfigurationError(
                config_item="max_cacheable_item_size",
                provided_value=str(max_cacheable_item_size),
                valid_value_description="A number greater than zero",
            )

        if max_evictions_per_query is not None and (
            not isinstance(max_evictions_per_query, int) or max_evictions_per_query <= 0
        ):
            raise InvalidConfigurationError(
                config_item="max_evictions_per_query",
                provided_value=str(max_evictions_per_query),
                valid_value_description="A number greater than zero",
            )

        if not isinstance(max_local_buffer_capacity, int) or max_local_buffer_capacity <= 0:
            raise InvalidConfigurationError(
                config_item="max_local_buffer_capacity",
                provided_value=str(max_local_buffer_capacity),
                valid_value_description="A number greater than zero",
            )

        self.cache_backend = cache_backend
        self.max_cacheable_item_size = max_cacheable_item_size
        self.max_evictions_per_query = max_evictions_per_query
        self.max_local_buffer_capacity = max_local_buffer_capacity
