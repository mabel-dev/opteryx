# faust-streaming-rocksdb

"""
This is opinionated for use as a metadata cache, it can be used as a reader cache
because they implement the same interface, but that would cache all of the files
locally, which is unlikely to be what is wanted.
"""

import io
from typing import Optional

from opteryx.managers.kvstores import BaseKeyValueStore


class LocalKVStore(BaseKeyValueStore):
    def __init__(self, **kwargs):
        import rocksdb

        location = kwargs.get("location", "metastore.rocksdb")
        self._db = rocksdb.DB(location, rocksdb.Options(create_if_missing=True))

    def get(self, key):
        byte_reponse = self._db.get(key.encode())
        if byte_reponse:
            return io.BytesIO(byte_reponse)
        return None

    def set(self, key, value):
        value.seek(0, 0)
        self._db.put(key.encode(), value.read())
        value.seek(0, 0)
        return None
