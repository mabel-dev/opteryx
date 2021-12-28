"""
StorageClassCompressedMemory is a helper class for persisting DictSets locally, it is
the backend for the COMPRESSED_MEMORY variation of the STORAGE CLASSES.

This is particularly useful when local storage is slow and the data is too big for
memory, or you are doing a lot of operations on the data.

If you're doing few operations on the the data and it is easily recreated or local
storage is fast, this isn't a good option.
"""
import orjson
import lz4.frame
from itertools import zip_longest
from . import BaseStorageClass

BATCH_SIZE = 500


class StorageClassCompressedMemory(BaseStorageClass):
    def __init__(self, iterable):
        compressor = lz4.frame

        self.batches = []
        self.length = 0

        batch = None
        for batch in zip_longest(*[iterable] * BATCH_SIZE):
            self.length += len(batch)
            # pay for fast deserialization here:
            if hasattr(batch[0], "mini"):
                json_batch = (
                    b"["
                    + b",".join([item.mini for item in batch if not item is None])
                    + b"]"
                )
                self.batches.append(compressor.compress(json_batch))
            else:
                self.batches.append(compressor.compress(orjson.dumps(batch)))

        # the last batch fills with Nones
        if batch:
            self.length -= batch.count(None)
        self.iterator = None

    def _inner_reader(self, *locations):
        decompressor = lz4.frame

        if locations:
            if not isinstance(locations, (tuple, list, set)):
                locations = [locations]
            ordered_location = sorted(locations)
            batch_number = -1
            batch = []
            for i in ordered_location:
                requested_batch = i % BATCH_SIZE
                if requested_batch != batch_number:
                    batch = self.parse_json(
                        decompressor.decompress(self.batches[requested_batch])
                    )
                yield batch[i - i % BATCH_SIZE]

        else:
            for batch in self.batches:
                records = self.parse_json(decompressor.decompress(batch))
                for record in records:
                    if record:
                        yield record

    def __iter__(self):
        self.iterator = iter(self._inner_reader())
        return self.iterator

    def __next__(self):
        if not self.iterator:
            self.iterator = iter(self._inner_reader())
        return next(self.iterator)

    def __len__(self):
        return self.length
