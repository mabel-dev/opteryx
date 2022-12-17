# TODO: rocks db

from opteryx.managers.kvstores import BaseKeyValueStore


class MongoDbKVStore(BaseKeyValueStore):
    def __init__(self):
        raise NotImplementedError("Mongo KV Store not implemented")
