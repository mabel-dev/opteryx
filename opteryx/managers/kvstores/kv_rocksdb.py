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

# faust-streaming-rocksdb

"""
This is opinionated for use as a metadata cache, it can be used as a reader cache
because they implement the same interface, but that would cache all of the files
locally, which is unlikely to be what is wanted.

"""
import io

from typing import Iterable

from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores import BaseKeyValueStore


ROCKS_DB = True

try:
    import rocksdb
except ImportError:
    ROCKS_DB = False


class RocksDB_KVStore(BaseKeyValueStore):
    def __init__(self, **kwargs):

        location = kwargs.get("location", "rocksdb.rocksdb")
        if not ROCKS_DB:
            raise MissingDependencyError(
                "`RocksDB` is missing, please install or include in requirements.txt"
            )
        self._db = rocksdb.DB(location, rocksdb.Options(create_if_missing=True))

    @staticmethod
    def can_use():
        return ROCKS_DB

    def get(self, key):
        if hasattr(key, "encode"):
            key = key.encode()
        byte_reponse = self._db.get(key)
        if byte_reponse:
            return io.BytesIO(byte_reponse)
        return None

    def set(self, key, value):
        if hasattr(key, "encode"):
            key = key.encode()
        value.seek(0, 0)
        self._db.put(key, value.read())
        value.seek(0, 0)

    def contains(self, keys: Iterable) -> Iterable:
        return [key for key in keys if self.get(key) is not None]
