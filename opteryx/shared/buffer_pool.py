from opteryx.managers.kvstores import InMemoryKVStore


class BufferPool(InMemoryKVStore):

    slots = "_kv"

    _kv = None

    def __new__(cls):
        if cls._kv is None:
            cls._kv = InMemoryKVStore(size=50)
        return cls._kv
