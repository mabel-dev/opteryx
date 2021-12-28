"""
StorageClassDisk is a helper class for persisting DictSets locally, it is the backend
for the DISK variation of the STORAGE CLASSES.

The Reader and Writer are pretty fast, the bottleneck is the parsing and serialization
of JSON data - this accounts for over 50% of the read/write times.
"""
import os
import mmap
import orjson
import atexit
from tempfile import NamedTemporaryFile
from ....utils.paths import silent_remove
from . import BaseStorageClass
from pyarrow import orc


class StorageClassOrc(BaseStorageClass):
    """
    This provides the reader for the DISK variation of STORAGE.
    """

    def __init__(self, iterator):
        self.file = NamedTemporaryFile(prefix="mabel-rel-", suffix=".orc").name
        atexit.register(silent_remove, filename=self.file)

    def _read_file(self):
        pass

    def _inner_reader(self, *locations):
        if locations:
            max_location = max(locations)
            min_location = min(locations)

            reader = self._read_file()

            for i in range(min_location):
                next(reader)

            for i, line in enumerate(reader, min_location):
                if i in locations:
                    yield self.parse_json(line)
                    if i == max_location:
                        return
        else:
            for line in self._read_file():
                yield self.parse_json(line)

    def __iter__(self):
        self.iterator = iter(self._inner_reader())
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = iter(self._inner_reader())
        return next(self.iterator)

    def __len__(self):
        return self.length

    def __del__(self):
        try:
            os.remove(self.file)
        except:  # nosec
            pass
