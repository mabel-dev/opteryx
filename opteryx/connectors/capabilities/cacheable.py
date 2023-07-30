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

from typing import Dict
from typing import Optional

from orso.cityhash import CityHash64


class Cacheable:
    """
    This implements a read-thru cache which wraps blob access routines.

    It intercepts requests to read blobs and first looks them up in the in-memory
    cache (the BufferPool) and optionally looks them up in a secondary cache -
    expected to be something like MemcacheD or Redis.

    This allows Connectors which access file/blob storage to not need to implement
    anything to take advantage of the caching, except put the @read_thru
    decorator around the low-level read call.
    """

    def __init__(self, cache=None, statistics: Optional[Dict[str, int]] = None, **kwargs):
        from opteryx.shared import BufferPool

        self.buffer_pool = BufferPool()
        self.statistics = statistics if statistics else {"cache_misses": 0, "cache_hits": 0}
        self.secondary_cache = cache

    def read_thru(self):
        """
        The read-thru cache read wraps file and blob-based reads
        """

        def decorator(func):
            def wrapper(*args, **kwargs):
                blob_name = kwargs["blob_name"]
                key = format(CityHash64(blob_name), "X")

                # Try to get the result from cache
                result = self.buffer_pool.get(key, self.secondary_cache)

                if result is None:
                    # Key is not in cache, execute the function and store the result in cache
                    result = func(*args, **kwargs)

                    # Write the result to cache
                    self.buffer_pool.set(key, result, self.secondary_cache)

                    self.statistics["cache_misses"] += 1
                else:
                    self.statistics["cache_hits"] += 1

                return result

            return wrapper

        return decorator
