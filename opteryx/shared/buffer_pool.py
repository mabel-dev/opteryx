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
Global Buffer Pool.

This is pretty naive at the moment.
"""

from opteryx.managers.kvstores import InMemoryKVStore


class BufferPool(InMemoryKVStore):

    _kv = None

    def __new__(cls):
        if cls._kv is None:
            cls._kv = InMemoryKVStore(size=50)
        return cls._kv
