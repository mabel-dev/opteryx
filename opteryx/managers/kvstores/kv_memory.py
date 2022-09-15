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
This implements an in-memory cache.

We use an LRU-K(2) to maintain the size of the cache, this is a variation of the naive
LRU algorithm.

We're using a dictionary and moving items to the top of the dictionary
when it's accessed. This relies on Python dictionaries being ordered.
"""

import io

from opteryx.managers.kvstores import BaseKeyValueStore
from opteryx.utils.lru_2 import LRU2


class InMemoryKVStore(BaseKeyValueStore):

    slots = ("_lru2",)

    def __init__(self, **kwargs):
        """
        Parameters:
            size: int (optional)
                The maximim number of items maintained in the cache.
        """
        size = int(kwargs.get("size", 50))
        self._lru2 = LRU2(size=size)

    def get(self, key):
        value = self._lru2.get(key)
        if value:
            return io.BytesIO(value)
        return None

    def set(self, key, value):
        value.seek(0, 0)
        ret = self._lru2.set(key, value.read())
        value.seek(0, 0)
        return ret

    def contains(self, keys):
        return list(set(keys).intersection(self._lru2.keys))
