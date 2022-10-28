# faust-streaming-rocksdb

"""
This is opinionated for use as a metadata cache, it can be used as a reader cache
because they implement the same interface, but that would cache all of the files
locally, which is unlikely to be what is wanted.

This will automatically fall back to the JSON store.
"""
import io

from opteryx.managers.kvstores import BaseKeyValueStore


ROCKS_DB = True

try:
    import rocksdb
except ImportError:
    ROCKS_DB = False


class LocalKVStore(BaseKeyValueStore):
    def __init__(self, **kwargs):

        location = kwargs.get("location", "metastore.rocksdb")
        if not ROCKS_DB:
            raise FeatureNotSupportedOnArchitectureError("RocksDB is not available")
        self._db = rocksdb.DB(location, rocksdb.Options(create_if_missing=True))

    @staticmethod
    def can_use(self):
        return ROCKS_DB

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
