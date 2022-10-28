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
This is a persisted JSON document as a KV store - this is a really bad idea, but
exists as a KV store of last resort. This is probably only going to be used on
Raspberry Pis and toy implementations of Opteryx.
"""

import base64
import io

import orjson

from opteryx.managers.kvstores import BaseKeyValueStore


class LocalKVJson(BaseKeyValueStore):
    def __init__(self, location):
        self._location = location

    def get(self, key):
        if hasattr(key, "decode"):
            key = key.decode()
        try:
            with open(self._location, mode="rb") as f:
                doc = orjson.loads(f.read())
                value = doc.get(key)
                if value:
                    value = base64.b85decode(value)
                    return io.BytesIO(value)
                return None
        except FileNotFoundError:
            return None

    def put(self, key, value):
        # for comatibility with RocksDB
        self.set(key, value)

    def set(self, key, value):
        if hasattr(key, "decode"):
            key = key.decode()

        if not value:
            bytes_value = None
        else:
            value.seek(0, 0)
            bytes_value = base64.b85encode(value.read()).decode()
            value.seek(0, 0)
        try:
            with open(self._location, mode="rb") as f:
                doc = orjson.loads(f.read())
        except FileNotFoundError:
            doc = {}
        doc.pop(key, None)
        if bytes_value:
            doc[key] = bytes_value
        with open(self._location, mode="wb") as f:
            f.write(orjson.dumps(doc))

    def contains(self, keys):
        if not isinstance(keys, (list, set, tuple)):
            keys = [keys]
        keys = [key.decode() if hasattr(key, "decode") else key for key in keys]
        try:
            with open(self._location, mode="r") as f:
                doc = orjson.loads(f.read())
            return [k for k in doc.keys() if k in keys]
        except FileNotFoundError:
            return []
