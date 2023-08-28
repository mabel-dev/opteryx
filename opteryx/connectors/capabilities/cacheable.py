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


from functools import wraps

from orso.cityhash import CityHash64

from opteryx.shared import BufferPool

buffer_pool = BufferPool()

__all__ = ("Cacheable", "read_thru_cache")


class Cacheable:
    """
    This class is just a marker - it is empty.

    Caching is added in the binding phase.
    """

    def __init__(self, *args, **kwargs):
        pass

    def read_blob(self, *, blob_name, **kwargs):
        pass


def read_thru_cache(func):
    """
    This implements a read-thru cache which wraps blob access routines.

    It intercepts requests to read blobs and first looks them up in the in-memory
    cache (the BufferPool) and optionally looks them up in a secondary cache -
    expected to be something like MemcacheD or Redis.

    This allows Connectors which access file/blob storage to not need to implement
    anything to take advantage of the caching, the binder adds this as a wrapper.
    """

    @wraps(func)
    def wrapper(*args, statistics, **kwargs):
        blob_name = kwargs["blob_name"]
        key = hex(CityHash64(blob_name))

        # Try to get the result from cache
        result = buffer_pool.get(key)

        if result is None:
            # Key is not in cache, execute the function and store the result in cache
            result = func(*args, **kwargs)

            # Write the result to cache
            buffer_pool.set(key, result)

            statistics.cache_misses += 1
        else:
            statistics.cache_hits += 1

        return result

    return wrapper
