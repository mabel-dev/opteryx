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
This is a Base class for a caching layer.

It's used by injecting an instantiated object into the Reader. If the object in None
we skip any caching, if it's set up, we use it as an aside cache.
"""
import abc
from typing import Optional


class BaseBufferCache(abc.ABC):
    def __init__(self, **kwargs):
        pass

    @abc.abstractclassmethod
    def get(sef, key: bytes) -> Optional[bytes]:
        """
        Overwrite this method to retrieve a value from the cache, or None if the
        value is not in the cache.
        """
        pass

    @abc.abstractclassmethod
    def set(self, key: bytes, value: bytes):
        """
        Overwrite this method to place a value in the cache.
        """
        pass
