from .cache_manager import CacheManager


# Only import the cache manager being used
def get_cache_manager(cache_type: str):
    """Return the cache manager class for the requested cache_type.

    This performs the import lazily so importing this package is cheaper when
    a backend is not needed.
    """
    if cache_type == "memcached":
        from .memcached import MemcachedCache

        return MemcachedCache
    elif cache_type == "redis":
        from .redis import RedisCache

        return RedisCache
    elif cache_type == "valkey":
        from .valkey import ValkeyCache

        return ValkeyCache
    else:
        from .null_cache import NullCache

        return NullCache


def __getattr__(name: str):
    """Lazily expose concrete cache manager classes as module attributes.

    This keeps `from opteryx.managers.cache import MemcachedCache` working
    while still avoiding eager imports of all backends.
    """
    if name == "MemcachedCache":
        from .memcached import MemcachedCache

        return MemcachedCache
    if name == "RedisCache":
        from .redis import RedisCache

        return RedisCache
    if name == "ValkeyCache":
        from .valkey import ValkeyCache

        return ValkeyCache
    if name == "NullCache":
        from .null_cache import NullCache

        return NullCache
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ("CacheManager", "get_cache_manager", "MemcachedCache", "RedisCache", "ValkeyCache", "NullCache")
