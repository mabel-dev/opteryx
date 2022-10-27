import orjson

from opteryx.managers.kvstores import BaseKeyValueStore


class LocalKVJson(BaseKeyValueStore):
    def __init__(self, loc):
        self._loc = loc

    def get(self, key):
        with open(self._loc, mode="rb") as f:
            doc = orjson.loads(f.read())
            return doc.get(key)

    def put(self, key, value):
        with open(self._loc, mode="rb") as f:
            doc = orjson.loads(f.read())
        doc[key] = value
        with open(self._loc, mode="wb") as f:
            f.write(orjson.dumps(doc))

    def contains(self, keys):
        if not isinstance(keys, (list, set, tuple)):
            keys = [keys]
        with open(self._loc, mode="rb") as f:
            doc = orjson.loads(f.read())
        return [k for k in doc.keys() if k in keys]
