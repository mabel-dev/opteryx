"""
Factory for creating KeyValueStore instances from URI-like locations.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore
from opteryx.managers.kvstores.file_kv_store import FileKeyValueStore
from opteryx.managers.kvstores.gcs_kv_store import GCSKeyValueStore
from opteryx.managers.kvstores.memcached import MemcachedCache
from opteryx.managers.kvstores.null_cache import NullCache
from opteryx.managers.kvstores.redis import RedisCache
from opteryx.managers.kvstores.s3_kv_store import S3KeyValueStore
from opteryx.managers.kvstores.valkey import ValkeyCache


def create_kv_store(location: str | None, **kwargs: Any) -> BaseKeyValueStore | None:
    """Create a suitable KeyValueStore based on a URI-like `location`.

    Accepts:
    - file:///path or /path
    - s3://bucket[/prefix]
    - gs://bucket[/prefix]
    - redis://host:port
    - memcached://host:port
    - valkey://connection
    - null://anything
    """
    if not location:
        return None

    parsed = urlparse(location)
    scheme = parsed.scheme or "file"

    if scheme in ("file", ""):  # plain file path
        return FileKeyValueStore(location)
    if scheme in ("s3", "minio"):
        return S3KeyValueStore(location, **kwargs)
    if scheme in ("gs", "gcs"):
        return GCSKeyValueStore(location, **kwargs)
    if scheme == "redis":
        # pass through server URL to redis.from_url handled in implementation
        return RedisCache(server=location, **kwargs)
    if scheme in ("memcached", "memcache"):
        server = parsed.netloc
        return MemcachedCache(server=server, **kwargs)
    if scheme == "valkey":
        server = parsed.netloc or location
        return ValkeyCache(server=server, **kwargs)
    if scheme == "null":
        return NullCache()

    raise ValueError(f"Unknown KV store scheme: {scheme}")
