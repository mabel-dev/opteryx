"""
Memcached-backed Key-Value Store moved into kvstores namespace.
"""

from __future__ import annotations

import os
from typing import Union

from orso.tools import single_item_cache

from opteryx.config import MAX_CONSECUTIVE_CACHE_FAILURES
from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores import BaseKeyValueStore


@single_item_cache
def _memcached_server(**kwargs):
    memcached_config = kwargs.get("server", os.environ.get("MEMCACHED_SERVER"))
    if memcached_config is None:
        return None

    memcached_config = memcached_config.split(":")
    if len(memcached_config) == 1:
        memcached_config.append(11211)

    if len(memcached_config) != 2:
        return None

    try:
        from pymemcache.client import base
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err

    try:
        cache = base.Client(
            (memcached_config[0], memcached_config[1]), connect_timeout=1, timeout=1
        )
    except Exception as err:
        print("[CACHE] Unable to create remote cache", err)
        cache = None

    return cache


class MemcachedCache(BaseKeyValueStore):
    hits: int = 0
    misses: int = 0
    skips: int = 0
    errors: int = 0
    sets: int = 0
    touches: int = 0

    def __init__(self, location: str | None = None, **kwargs):
        self._server = _memcached_server(**kwargs)
        super().__init__(location)
        if self._server is None:
            import datetime

            print(f"{datetime.datetime.now()} [CACHE] Unable to set up memcached cache.")
            self._consecutive_failures: int = MAX_CONSECUTIVE_CACHE_FAILURES
        else:
            self._consecutive_failures = 0

    def get(self, key: bytes) -> Union[bytes, None]:
        if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
            self.skips += 1
            return None
        try:
            response = self._server.get(bytes(key))
            self._consecutive_failures = 0
            if response:
                self.hits += 1
                return bytes(response)
        except Exception as err:  # pragma: no cover
            self._consecutive_failures += 1
            if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Memcached cache due to persistent errors ({err}) [GET]."
                )
            self.errors += 1
            return None

        self.misses += 1
        return None

    def set(self, key: bytes, value: bytes) -> None:
        if self._consecutive_failures < MAX_CONSECUTIVE_CACHE_FAILURES:
            try:
                self._server.set(bytes(key), value)
                self.sets += 1
            except Exception as err:
                self._consecutive_failures = MAX_CONSECUTIVE_CACHE_FAILURES
                self.errors += 1
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Memcached cache due to persistent errors ({err}) [SET]."
                )
        else:
            self.skips += 1

    def touch(self, key: bytes):
        if self._consecutive_failures < MAX_CONSECUTIVE_CACHE_FAILURES:
            try:
                self._server.touch(bytes(key))
                self.touches += 1
            except Exception as err:  # nosec
                if not err:
                    pass
                self.errors += 1
                pass

    def delete(self, key: bytes) -> None:
        try:
            self._server.delete(bytes(key))
        except Exception as err:
            self.errors += 1
