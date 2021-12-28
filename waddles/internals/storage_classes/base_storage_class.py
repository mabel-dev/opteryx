from abc import ABC, abstractclassmethod

try:
    import simdjson

    def json(ds):
        """parse each line in the file to a dictionary"""
        json_parser = simdjson.Parser()
        return json_parser.parse(ds)


except ImportError:
    import orjson

    def json(ds):
        return orjson.loads(ds)


class BaseStorageClass(ABC):

    iterator = None
    length = -1

    @abstractclassmethod
    def __init__(self, iterator):
        pass

    @abstractclassmethod
    def _inner_reader(self, *locations):
        pass

    def __iter__(self):
        self.iterator = self._inner_reader()
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = self._inner_reader()
        return next(self.iterator)

    def __len__(self):
        return self.length

    def parse_json(self, ds):
        return json(ds)
