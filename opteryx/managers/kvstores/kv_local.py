# TODO: rocks db
# faust-streaming-rocksdb

import io
from typing import Optional

from opteryx.managers.kvstores import BaseKeyValueStore


class LocalKVStore(BaseKeyValueStore):
    
    def __init__(self, **kwargs):
        import rocksdb

        location = kwargs.get("location", "metastore.rocksdb")
        self._size = kwargs.get("size", 512)
        self._db = rocksdb.DB(location, rocksdb.Options(create_if_missing=True))


    def get(self, key: bytes) -> Optional[bytes]:
        byte_reponse = self._db.get(key.encode())
        if byte_reponse:
            return io.BytesIO(byte_reponse)
        return None

    def set(self, key: bytes, value: bytes):
        print(self._db.get_property(b"rocksdb.stats"))
        value.seek(0, 0)
        self._db.put(key.encode(), value.read())
        value.seek(0, 0)
