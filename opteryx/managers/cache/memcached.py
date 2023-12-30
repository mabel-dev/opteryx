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
This implements an interface to Memcached

If we have 10 failures in a row, stop trying to use the cache.
"""

import os
from typing import Union

from orso.tools import single_item_cache

from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores import BaseKeyValueStore

MAXIMUM_CONSECUTIVE_FAILURES: int = 10


@single_item_cache
def _memcached_server(**kwargs):
    """
    Handling connecting to Memcached
    """
    # the server must be set in the environment
    memcached_servers = kwargs.get("servers", os.environ.get("MEMCACHED_SERVERS", "")).split(",")
    memcached_username = kwargs.get("username", os.environ.get("MEMCACHED_USERNAME", ""))
    memcached_password = kwargs.get("password", os.environ.get("MEMCACHED_PASSWORD", ""))

    try:
        import bmemcached
        import pymemcache
    except ImportError as err:
        raise MissingDependencyError(err.name) from err

    try:
        cache = bmemcached.Client(
            memcached_servers,
            username=memcached_username,
            password=memcached_password,
            socket_timeout=1,
        )
        cache.enable_retry_delay(True)
        cache.set(memcached_username, memcached_password)
        print(cache.get("key"))
    except Exception as err:
        print("[CACHE] Unable to create remote cache", err)
        cache = None

    return cache


class MemcachedCache(BaseKeyValueStore):
    """
    Cache object
    """

    def __init__(self, **kwargs):
        """
        Parameters:
            servers: string (optional)
                Sets the memcached server and port (server:port). If not provided
                the value will be obtained from the OS environment.
        """
        self._server = _memcached_server(**kwargs)
        if self._server is None:
            self._consecutive_failures: int = MAXIMUM_CONSECUTIVE_FAILURES
        else:
            self._consecutive_failures: int = 0
        self.hits: int = 0
        self.misses: int = 0
        self.skips: int = 0
        self.errors: int = 0

    def get(self, key: str) -> Union[bytes, None]:
        if self._consecutive_failures >= MAXIMUM_CONSECUTIVE_FAILURES:
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
            if self._consecutive_failures >= MAXIMUM_CONSECUTIVE_FAILURES:
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Memcached cache due to persistent errors ({err})."
                )
            self.errors += 1
            return None

        self.misses += 1
        return None

    def set(self, key: str, value: bytes) -> None:
        if self._consecutive_failures < MAXIMUM_CONSECUTIVE_FAILURES:
            self._server.set(key, value)
