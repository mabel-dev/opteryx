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
This is a Base class for KV Value Storage adapter.

This is used by the metadata store and in-memory buffer cache. 
"""
import abc
from typing import Optional


class BaseKeyValueStore(abc.ABC):
    """
    Base class for cache objects
    """

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Overwrite this method to retrieve a value from the cache, or None if the
        value is not in the cache.
        """
        raise NotImplementedError("`get` method on cache object not overridden.")

    def set(self, key: bytes, value: bytes):
        """
        Overwrite this method to place a value in the cache.
        """
        raise NotImplementedError("`set` method on cache object not overridden.")

    def contains(self, lst):
        """
        Overwrite this method to return a list of itmes which are in the cache from
        a given list
        """
        # default to returning no matches
        return []
