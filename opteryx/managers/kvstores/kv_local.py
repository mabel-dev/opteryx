# faust-streaming-rocksdb

"""
This is opinionated for use as a metadata cache, it can be used as a reader cache
because they implement the same interface, but that would cache all of the files
locally, which is unlikely to be what is wanted.
"""
import io

from opteryx.managers.kvstores import BaseKeyValueStore
from opteryx.managers.kvstores import LocalKVJson


ROCKS_DB = True

try:
    import rocksdb
except ImportError:
    ROCKS_DB = False


class LocalKVStore(BaseKeyValueStore):
    def __init__(self, **kwargs):

        location = kwargs.get("location", "metastore.rocksdb")
        if ROCKS_DB:
            self._db = rocksdb.DB(location, rocksdb.Options(create_if_missing=True))
        else:
            self._db = LocalKVJson(location + ".json")

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
