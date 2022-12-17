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

from .base_kv_store import BaseKeyValueStore

from .kv_firestore import FireStoreKVStore
from .kv_memory import InMemoryKVStore
from .kv_local_json import LocalKVJson
from .kv_memcached import MemcachedKVStore
from .kv_mongodb import MongoDbKVStore
from .kv_rocksdb import RocksDB_KVStore


def KV_store_factory(store):
    """
    A factory method for getting KV Store instances
    """
    stores = {
        "firestore": FireStoreKVStore,
        "memory": InMemoryKVStore,
        "json": LocalKVJson,
        "memcached": MemcachedKVStore,
        "mongodb": MongoDbKVStore,
        "rocksdb": RocksDB_KVStore,
    }

    return stores.get(store.lower(), LocalKVJson)
