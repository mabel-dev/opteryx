from . import BaseStorageClass


class StorageClassMemory(BaseStorageClass):
    """
    This provides the reader for the MEMORY variation of STORAGE.
    """

    def __init__(self, iterator):
        self.inner_reader = None
        self.data = list(iterator)
        self.length = len(self.data)
        self.iterator = None

    def _inner_reader(self, *locations):
        if locations:
            yield from [self.data[l] for l in locations]
        else:
            yield from self.data

    def __iter__(self):
        self.iterator = self._inner_reader()
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = self._inner_reader()
        return next(self.iterator)

    def __len__(self):
        return self.length
