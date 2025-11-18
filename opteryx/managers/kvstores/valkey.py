"""
Valkey-backed Key-Value Store moved into kvstores namespace.
"""

from __future__ import annotations

import os
from typing import Union

from orso.tools import single_item_cache

from opteryx.config import MAX_CONSECUTIVE_CACHE_FAILURES
from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores import BaseKeyValueStore


@single_item_cache
def _valkey_server(**kwargs):
    valkey_config = kwargs.get("server", os.environ.get("VALKEY_CONNECTION"))
    if valkey_config is None:
        return None

    try:
        import valkey
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err

    return valkey.from_url(valkey_config)


class ValkeyCache(BaseKeyValueStore):
    hits: int = 0
    misses: int = 0
    skips: int = 0
    errors: int = 0
    sets: int = 0

    def __init__(self, location: str | None = None, **kwargs):
        self._server = _valkey_server(**kwargs)
        super().__init__(location)
        if self._server is None:
            import datetime

            print(f"{datetime.datetime.now()} [CACHE] Unable to set up valkey cache.")
            self._consecutive_failures: int = MAX_CONSECUTIVE_CACHE_FAILURES
        else:
            self._consecutive_failures = 0

    def get(self, key: bytes) -> Union[bytes, None]:
        if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
            self.skips += 1
            return None
        try:
            response = self._server.get(key)
            self._consecutive_failures = 0
            if response:
                self.hits += 1
                return bytes(response)
        except Exception as err:  # pragma: no cover
            self._consecutive_failures += 1
            if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Valkey cache due to persistent errors ({err})."
                )
            self.errors += 1
            return None

        self.misses += 1
        return None

    def set(self, key: bytes, value: bytes) -> None:
        if self._consecutive_failures < MAX_CONSECUTIVE_CACHE_FAILURES:
            try:
                self._server.set(key, value)
                self.sets += 1
            except Exception as err:  # pragma: no cover
                self._consecutive_failures = MAX_CONSECUTIVE_CACHE_FAILURES
                self.errors += 1
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Valkey cache due to persistent errors ({err}) [SET]."
                )
        else:
            self.skips += 1

    def delete(self, key):
        try:
            self._server.delete(key)
        except Exception as err:
            self.errors += 1
